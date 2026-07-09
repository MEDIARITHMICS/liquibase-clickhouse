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

import liquibase.changelog.ChangeLogHistoryServiceFactory;
import liquibase.database.ObjectQuotingStrategy;
import liquibase.database.core.MSSQLDatabase;
import liquibase.database.core.MySQLDatabase;
import liquibase.exception.DatabaseException;
import liquibase.exception.LockException;
import liquibase.executor.jvm.ChangelogJdbcMdcListener;
import liquibase.ext.clickhouse.database.ClickHouseDatabase;

import liquibase.Scope;
import liquibase.database.Database;
import liquibase.exception.LiquibaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.lockservice.StandardLockService;
import liquibase.logging.Logger;
import liquibase.logging.mdc.MdcKey;
import liquibase.logging.mdc.MdcObject;
import liquibase.logging.mdc.MdcValue;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.LockDatabaseChangeLogStatement;
import liquibase.statement.core.RawParameterizedSqlStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.statement.core.SelectFromDatabaseChangeLogLockStatement;
import liquibase.statement.core.UnlockDatabaseChangeLogStatement;

import java.util.logging.Level;

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
  public boolean isDatabaseChangeLogLockTableInitialized(boolean tableJustCreated, boolean forceRecheck) {
    if (!isLockTableInitialized || forceRecheck) {
      try {
        String query =
            String.format(
                "SELECT COUNT(*) FROM `%s`.%s",
                database.getDefaultSchemaName(), database.getDatabaseChangeLogLockTableName());
        int nbRows = getExecutor().queryForInt(new RawParameterizedSqlStatement(query));
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
  public boolean acquireLock() throws LockException {
    if (hasChangeLogLock) {
      return true;
    }

    quotingStrategy = database.getObjectQuotingStrategy();

    Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database);

    try {
      database.rollback();
      this.init();
      SqlStatement lockedStatement = new SelectFromDatabaseChangeLogLockStatement("LOCKED");
      Boolean locked = ChangelogJdbcMdcListener.query(database, ex -> ex.queryForObject(lockedStatement, Boolean.class));

      if (locked) {
        return false;
      } else {

        executor.comment("Lock Database");
        SqlStatement lockDatabaseStatement = new LockDatabaseChangeLogStatement();
        ChangelogJdbcMdcListener.query(database, ex -> ex.update(lockDatabaseStatement));
        database.commit();
        Scope.getCurrentScope().getLog(getClass()).info(coreBundle.getString("successfully.acquired.change.log.lock"));

        hasChangeLogLock = true;

        Scope.getCurrentScope().getSingleton(ChangeLogHistoryServiceFactory.class).resetAll();
        database.setCanCacheLiquibaseTableInfo(true);
        return true;
      }
    } catch (Exception e) {
      throw new LockException(e);
    } finally {
      try {
        database.rollback();
      } catch (DatabaseException e) {
        Scope.getCurrentScope().getLog(getClass()).warning("Failed to rollback", e);
      }
    }
  }

  @Override
  public void releaseLock() throws LockException {
    ObjectQuotingStrategy incomingQuotingStrategy = null;
    if (this.quotingStrategy != null) {
      incomingQuotingStrategy = database.getObjectQuotingStrategy();
      database.setObjectQuotingStrategy(this.quotingStrategy);
    }

    boolean success = false;
    Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database);
    try {
      if (this.isDatabaseChangeLogLockTableCreated()) {
        executor.comment("Release Database Lock");
        database.rollback();
        SqlStatement unlockStatement = new UnlockDatabaseChangeLogStatement();
        ChangelogJdbcMdcListener.query(database, ex -> ex.update(unlockStatement));
        database.commit();
        success = true;
      }
    } catch (Exception e) {
      throw new LockException(e);
    } finally {
      try {
        hasChangeLogLock = false;

        database.setCanCacheLiquibaseTableInfo(false);
        try (
            MdcObject releaseLocksOutcome = Scope.getCurrentScope().addMdcValue(MdcKey.RELEASE_LOCKS_OUTCOME, success ? MdcValue.COMMAND_SUCCESSFUL : MdcValue.COMMAND_FAILED)) {
          Scope.getCurrentScope().getLog(getClass()).log(success ? Level.INFO : Level.WARNING, (success ? "Successfully released" : "Failed to release") + " change log lock", null);
        }

        database.rollback();
      } catch (DatabaseException e) {
        Scope.getCurrentScope().getLog(getClass()).warning("Failed to rollback", e);
      }
      if (incomingQuotingStrategy != null) {
        database.setObjectQuotingStrategy(incomingQuotingStrategy);
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
