/*-
 * #%L
 * Liquibase extension for Clickhouse
 * %%
 * Copyright (C) 2020 - 2022 Mediarithmics
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package liquibase.ext.clickhouse.lockservice;

import liquibase.ext.clickhouse.database.ClickHouseDatabase;

import liquibase.Scope;
import liquibase.changelog.ChangeLogHistoryServiceFactory;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.exception.LockException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.lockservice.StandardLockService;
import liquibase.logging.Logger;
import liquibase.statement.core.LockDatabaseChangeLogStatement;
import liquibase.statement.core.RawSqlStatement;

public class ClickHouseLockService extends StandardLockService {

  private boolean isLockTableInitialized;

  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }

  @Override
  public boolean supports(Database database) {
    return database instanceof ClickHouseDatabase;
  }

  @Override
  public boolean isDatabaseChangeLogLockTableInitialized(boolean tableJustCreated) {
    if (!isLockTableInitialized) {
      try {
        String query =
            String.format(
                "SELECT COUNT(*) FROM %s.%s",
                database.getDefaultSchemaName(), database.getDatabaseChangeLogLockTableName());
        int nbRows = getExecutor().queryForInt(new RawSqlStatement(query));
        isLockTableInitialized = nbRows > 0;
      } catch (LiquibaseException e) {
        if (getExecutor().updatesDatabase()) {
          throw new UnexpectedLiquibaseException(e);
        } else {
          isLockTableInitialized = !tableJustCreated;
        }
      }
    }
    return isLockTableInitialized;
  }

  @Override
  public boolean hasDatabaseChangeLogLockTable(boolean forceRecheck) {
    if (forceRecheck || hasDatabaseChangeLogLockTable == null) {
      try {
        String query =
            String.format(
                "SELECT ID FROM %s.%s LIMIT 1",
                database.getDefaultSchemaName(), database.getDatabaseChangeLogLockTableName());
        getExecutor().execute(new RawSqlStatement(query));
        hasDatabaseChangeLogLockTable = true;
      } catch (DatabaseException e) {
        getLogger()
            .info(
                String.format(
                    "No %s table available", database.getDatabaseChangeLogLockTableName()));
        hasDatabaseChangeLogLockTable = false;
      }
    }
    return hasDatabaseChangeLogLockTable;
  }

  @Override
  public boolean acquireLock() throws LockException {
    if (hasChangeLogLock) {
      return true;
    }

    quotingStrategy = database.getObjectQuotingStrategy();

    Executor executor =
        Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database);

    try {
      database.rollback();
      this.init();

      String query =
          String.format(
              "SELECT MAX(LOCKED) FROM %s.%s",
              database.getDefaultSchemaName(), database.getDatabaseChangeLogLockTableName());
      boolean locked = executor.queryForInt(new RawSqlStatement(query)) > 0;

      if (locked) {
        return false;
      } else {

        executor.comment("Lock Database");
        int rowsUpdated = executor.update(new LockDatabaseChangeLogStatement());

        if (rowsUpdated > 1) {
          throw new LockException("Did not update change log lock correctly");
        }
        if (rowsUpdated == 0) {
          // another node was faster
          return false;
        }
        database.commit();
        Scope.getCurrentScope()
            .getLog(getClass())
            .info(coreBundle.getString("successfully.acquired.change.log.lock"));

        hasChangeLogLock = true;

        ChangeLogHistoryServiceFactory.getInstance().resetAll();
        database.setCanCacheLiquibaseTableInfo(true);
        return true;
      }
    } catch (Exception e) {
      throw new LockException(e);
    } finally {
      try {
        database.rollback();
      } catch (DatabaseException ignored) {

      }
    }
  }

  private Executor getExecutor() {
    return Scope.getCurrentScope()
        .getSingleton(ExecutorService.class)
        .getExecutor("jdbc", database);
  }

  private Logger getLogger() {
    return Scope.getCurrentScope().getLog(ClickHouseLockService.class);
  }
}
