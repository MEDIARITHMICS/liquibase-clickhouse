/*-
 * #%L
 * Liquibase extension for Clickhouse
 * %%
 * Copyright (C) 2020 - 2023 Mediarithmics
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
import liquibase.ext.clickhouse.sqlgenerator.LockDatabaseChangeLogClickHouse;

import liquibase.Scope;
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
import liquibase.statement.core.SelectFromDatabaseChangeLogLockStatement;
import liquibase.statement.core.UnlockDatabaseChangeLogStatement;

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
                "SELECT COUNT(*) FROM `%s`.%s",
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

  // StandardLockService relies on the update count of the lock/unlock statements, but ClickHouse
  // drivers do not report one for ALTER ... UPDATE (com.clickhouse returns 0). Read the lock row
  // back instead.
  @Override
  public boolean acquireLock() throws LockException {
    if (hasChangeLogLock) {
      return true;
    }

    Executor executor = getExecutor();
    try {
      database.rollback();
      init();

      if (isLocked(executor)) {
        return false;
      }

      executor.comment("Lock Database");
      executor.update(new LockDatabaseChangeLogStatement());
      String lockedBy =
          executor.queryForObject(
              new SelectFromDatabaseChangeLogLockStatement("LOCKEDBY"), String.class);
      if (!LockDatabaseChangeLogClickHouse.LOCKED_BY.equals(lockedBy)) {
        // another process was faster
        return false;
      }
      database.commit();
      getLogger().info("Successfully acquired change log lock");

      hasChangeLogLock = true;
      database.setCanCacheLiquibaseTableInfo(true);
      return true;
    } catch (Exception e) {
      throw new LockException(e);
    } finally {
      try {
        database.rollback();
      } catch (DatabaseException e) {
        // nothing to roll back
      }
    }
  }

  @Override
  public void releaseLock() throws LockException {
    Executor executor = getExecutor();
    try {
      if (hasDatabaseChangeLogLockTable()) {
        executor.comment("Release Database Lock");
        database.rollback();
        executor.update(new UnlockDatabaseChangeLogStatement());
        if (isLocked(executor)) {
          throw new LockException("Did not update change log lock correctly");
        }
        database.commit();
      }
    } catch (Exception e) {
      throw new LockException(e);
    } finally {
      try {
        hasChangeLogLock = false;
        database.setCanCacheLiquibaseTableInfo(false);
        getLogger().info("Successfully released change log lock");
        database.rollback();
      } catch (DatabaseException e) {
        // nothing to roll back
      }
    }
  }

  private boolean isLocked(Executor executor) throws DatabaseException {
    Boolean locked =
        executor.queryForObject(
            new SelectFromDatabaseChangeLogLockStatement("LOCKED"), Boolean.class);
    return Boolean.TRUE.equals(locked);
  }

  @Override
  public boolean hasDatabaseChangeLogLockTable() {
    boolean hasTable = false;
    try {
      String query =
          String.format(
              "SELECT ID FROM `%s`.%s LIMIT 1",
              database.getDefaultSchemaName(), database.getDatabaseChangeLogLockTableName());
      getExecutor().execute(new RawSqlStatement(query));
      hasTable = true;
    } catch (DatabaseException e) {
      getLogger()
          .info(
              String.format("No %s table available", database.getDatabaseChangeLogLockTableName()));
    }
    return hasTable;
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
