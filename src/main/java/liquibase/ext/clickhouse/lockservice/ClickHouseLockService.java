/*-
 * #%L
 * Liquibase extension for Clickhouse
 * %%
 * Copyright (C) 2020 - 2026 Mediarithmics
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

  @Override
  public boolean acquireLock() throws LockException {
    if (hasChangeLogLock) {
      return true;
    }

    try {
      // Ensure lock table exists and is initialized before attempting to acquire
      if (!hasDatabaseChangeLogLockTable()) {
        getLogger().info("Lock table does not exist, initializing...");
        super.init();
      }

      // Check if lock is already held before attempting to acquire
      String checkQuery =
          String.format(
              "SELECT LOCKED FROM `%s`.%s WHERE ID = 1",
              database.getDefaultSchemaName(), database.getDatabaseChangeLogLockTableName());

      int lockedBefore = getExecutor().queryForInt(new RawSqlStatement(checkQuery));

      if (lockedBefore == 1) {
        // Lock is already held by another process
        return false;
      }

      // Execute the lock statement (ALTER TABLE UPDATE doesn't return meaningful affected rows in ClickHouse)
      getExecutor().execute(new LockDatabaseChangeLogStatement());

      // Verify lock was acquired by checking LOCKED=1 after mutation
      // mutations_sync=1 in the lock statement ensures data is visible
      int lockedAfter = getExecutor().queryForInt(new RawSqlStatement(checkQuery));

      if (lockedAfter == 1) {
        hasChangeLogLock = true;
        getLogger().info("Successfully acquired change log lock");
        return true;
      }
      return false;
    } catch (LiquibaseException e) {
      throw new LockException(e);
    }
  }

  @Override
  public void releaseLock() throws LockException {
    try {
      // Execute the unlock statement (ALTER TABLE UPDATE doesn't return meaningful affected rows in ClickHouse)
      getExecutor().execute(new UnlockDatabaseChangeLogStatement());
      hasChangeLogLock = false;
      getLogger().info("Successfully released change log lock");
    } catch (LiquibaseException e) {
      throw new LockException(e);
    }
  }
}
