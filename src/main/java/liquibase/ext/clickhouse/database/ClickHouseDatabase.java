/*-
 * #%L
 * Liquibase extension for Clickhouse
 * %%
 * Copyright (C) 2020 Mediarithmics
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
package liquibase.ext.clickhouse.database;

import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;
import ru.yandex.clickhouse.ClickHouseDriver;

public class ClickHouseDatabase extends AbstractJdbcDatabase {

  private static final String DATABASE_NAME = "ClickHouse";
  private static final int DEFAULT_PORT = 8123;
  private static final String DRIVER_CLASS_NAME = ClickHouseDriver.class.getName();
  private static final String CURRENT_DATE_TIME_FUNCTION = "now()";

  public ClickHouseDatabase() {
    super();
    this.setCurrentDateTimeFunction(CURRENT_DATE_TIME_FUNCTION);
  }

  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }

  @Override
  protected String getDefaultDatabaseProductName() {
    return DATABASE_NAME;
  }

  @Override
  public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) throws DatabaseException {
    return DATABASE_NAME.equals(conn.getDatabaseProductName());
  }

  @Override
  public String getDefaultDriver(String url) {
    return url != null && url.startsWith("jdbc:clickhouse") ? DRIVER_CLASS_NAME : null;
  }

  @Override
  public String getShortName() {
    return "clickhouse";
  }

  @Override
  public Integer getDefaultPort() {
    return DEFAULT_PORT;
  }

  @Override
  public boolean supportsInitiallyDeferrableColumns() {
    return false;
  }

  @Override
  public boolean supportsTablespaces() {
    return false;
  }

  @Override
  public boolean supportsSequences() {
    return false;
  }

  @Override
  public boolean supportsSchemas() {
    return false;
  }
}
