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
package liquibase.ext.clickhouse.sqlgenerator;

import liquibase.ext.clickhouse.database.ClickHouseDatabase;
import liquibase.ext.clickhouse.params.ClusterConfig;
import liquibase.ext.clickhouse.params.ParamsLoader;

import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.UnlockDatabaseChangeLogGenerator;
import liquibase.statement.core.UnlockDatabaseChangeLogStatement;

public class UnlockDatabaseChangelogClickHouse extends UnlockDatabaseChangeLogGenerator {

  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }

  @Override
  public boolean supports(UnlockDatabaseChangeLogStatement statement, Database database) {
    return database instanceof ClickHouseDatabase;
  }

  @Override
  public Sql[] generateSql(
      UnlockDatabaseChangeLogStatement statement,
      Database database,
      SqlGeneratorChain sqlGeneratorChain) {
    ClusterConfig properties = ParamsLoader.getLiquibaseClickhouseProperties();

    String unlockQuery =
        String.format(
            "ALTER TABLE `%s`.%s "
                + SqlGeneratorUtil.generateSqlOnClusterClause(properties)
                + "UPDATE LOCKED = 0,LOCKEDBY = null, LOCKGRANTED = null WHERE ID = 1 AND LOCKED = 1 SETTINGS mutations_sync = 1",
            database.getDefaultSchemaName(),
            database.getDatabaseChangeLogLockTableName());

    return SqlGeneratorUtil.generateSql(database, unlockQuery);
  }
}
