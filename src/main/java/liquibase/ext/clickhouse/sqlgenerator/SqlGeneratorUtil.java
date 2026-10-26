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

import java.util.*;

import liquibase.ext.clickhouse.params.ClusterConfig;

import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.core.RawSqlStatement;

class SqlGeneratorUtil {

  public static Sql[] generateSql(Database database, String... statements) {
    SqlGeneratorFactory sqlGeneratorFactory = SqlGeneratorFactory.getInstance();
    List<Sql> allSqlStatements = new ArrayList<>();
    for (String statement : statements) {
      RawSqlStatement rawSqlStatement = new RawSqlStatement(statement);
      Sql[] perStatement = sqlGeneratorFactory.generateSql(rawSqlStatement, database);
      allSqlStatements.addAll(Arrays.asList(perStatement));
    }
    return allSqlStatements.toArray(new Sql[0]);
  }

  public static String generateSqlOnClusterClause(ClusterConfig properties) {
    if (properties != null) return String.format("ON CLUSTER '%s' ", properties.getClusterName());
    else return " ";
  }

  public static String generateSqlEngineClause(ClusterConfig properties, String tableName) {
    if (properties != null)
      return String.format(
          "ENGINE ReplicatedMergeTree('%s','%s') ORDER BY ID",
          properties.getTableZooKeeperPathPrefix() + tableName.toLowerCase(Locale.ROOT),
          properties.getTableReplicaName());
    else return "ENGINE MergeTree() ORDER BY ID";
  }
}
