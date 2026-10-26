/*-
 * #%L
 * Liquibase extension for ClickHouse
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
import liquibase.datatype.DataTypeFactory;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.TagDatabaseGenerator;
import liquibase.statement.core.TagDatabaseStatement;
import liquibase.structure.core.Column;

public class TagDatabaseGeneratorClickhouse extends TagDatabaseGenerator {
  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }

  @Override
  public boolean supports(TagDatabaseStatement statement, Database database) {
    return database instanceof ClickHouseDatabase;
  }

  @Override
  public Sql[] generateSql(
      TagDatabaseStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
    String tableNameEscaped =
        database.escapeTableName(
            database.getLiquibaseCatalogName(),
            database.getLiquibaseSchemaName(),
            database.getDatabaseChangeLogTableName());
    ClusterConfig properties = ParamsLoader.getLiquibaseClickhouseProperties();
    String orderColumnNameEscaped = database.escapeObjectName("ORDEREXECUTED", Column.class);
    String dateColumnNameEscaped = database.escapeObjectName("DATEEXECUTED", Column.class);
    String tagEscaped =
        DataTypeFactory.getInstance()
            .fromObject(statement.getTag(), database)
            .objectToSql(statement.getTag(), database);

    return new Sql[] {
      new UnparsedSql(
          "ALTER TABLE "
              + tableNameEscaped
              + SqlGeneratorUtil.generateSqlOnClusterClause(properties)
              + " UPDATE TAG="
              + tagEscaped
              + " WHERE "
              + dateColumnNameEscaped
              + "=(SELECT "
              + dateColumnNameEscaped
              + " FROM "
              + tableNameEscaped
              + " ORDER BY "
              + dateColumnNameEscaped
              + " DESC, "
              + orderColumnNameEscaped
              + " DESC LIMIT 1)"
              + " AND "
              + orderColumnNameEscaped
              + "=(SELECT "
              + orderColumnNameEscaped
              + " FROM "
              + tableNameEscaped
              + " ORDER BY "
              + dateColumnNameEscaped
              + " DESC, "
              + orderColumnNameEscaped
              + " DESC LIMIT 1) SETTINGS mutations_sync = 1")
    };
  }
}
