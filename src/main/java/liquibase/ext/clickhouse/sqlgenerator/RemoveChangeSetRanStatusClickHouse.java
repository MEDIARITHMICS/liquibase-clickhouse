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

import liquibase.changelog.ChangeSet;
import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.RemoveChangeSetRanStatusGenerator;
import liquibase.statement.core.RemoveChangeSetRanStatusStatement;

public class RemoveChangeSetRanStatusClickHouse extends RemoveChangeSetRanStatusGenerator {

  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }

  @Override
  public boolean supports(RemoveChangeSetRanStatusStatement statement, Database database) {
    return database instanceof ClickHouseDatabase;
  }

  @Override
  public Sql[] generateSql(
      RemoveChangeSetRanStatusStatement statement,
      Database database,
      SqlGeneratorChain sqlGeneratorChain) {
    ClusterConfig properties = ParamsLoader.getLiquibaseClickhouseProperties();

    ChangeSet changeSet = statement.getChangeSet();
    String unlockQuery =
        String.format(
            "ALTER TABLE `%s`.%s "
                + SqlGeneratorUtil.generateSqlOnClusterClause(properties)
                + "DELETE WHERE ID = '%s' AND AUTHOR = '%s' AND FILENAME = '%s' SETTINGS mutations_sync = 1",
            database.getDefaultSchemaName(),
            database.getDatabaseChangeLogTableName(),
            changeSet.getId(),
            changeSet.getAuthor(),
            changeSet.getFilePath());
    return SqlGeneratorUtil.generateSql(database, unlockQuery);
  }
}
