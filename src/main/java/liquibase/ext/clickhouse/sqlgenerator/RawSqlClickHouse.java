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

import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.RawSqlGenerator;
import liquibase.statement.core.RawSqlStatement;
import liquibase.util.StringUtil;

/**
 * ClickHouse executes one statement per request, so raw SQL (formatted SQL changesets, sql and
 * sqlFile changes) is split into statements here instead of relying on the JDBC driver. This also
 * covers changesets parsed with splitStatements:false, e.g. Liquibase applying
 * rollbackSplitStatements:false to the changeset itself.
 */
public class RawSqlClickHouse extends RawSqlGenerator {

  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }

  @Override
  public boolean supports(RawSqlStatement statement, Database database) {
    return database instanceof ClickHouseDatabase;
  }

  @Override
  public Sql[] generateSql(
      RawSqlStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
    String[] statements =
        StringUtil.processMultiLineSQL(statement.getSql(), true, true, statement.getEndDelimiter());
    if (statements.length <= 1) {
      return super.generateSql(statement, database, sqlGeneratorChain);
    }

    Sql[] result = new Sql[statements.length];
    for (int i = 0; i < statements.length; i++) {
      result[i] = new UnparsedSql(statements[i], statement.getEndDelimiter());
    }
    return result;
  }
}
