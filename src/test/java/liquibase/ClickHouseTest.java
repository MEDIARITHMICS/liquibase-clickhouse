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
package liquibase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.ResourceAccessor;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.apache.commons.io.output.NullWriter;

@Testcontainers
public class ClickHouseTest {

  @Container
  private static ClickHouseContainer clickHouseContainer =
      new ClickHouseContainer("clickhouse/clickhouse-server:26.8.4.11")
          // recent images refuse network access for the passwordless default user otherwise
          .withEnv("CLICKHOUSE_SKIP_USER_SETUP", "1");

  @Test
  void canInitializeLiquibaseSchema() {
    runLiquibase("empty-changelog.xml", (liquibase, database) -> liquibase.update(""));
  }

  @Test
  void canExecuteChangelog() {
    runLiquibase(
        "changelog.xml",
        (liquibase, database) -> {
          liquibase.update("");
          liquibase.update(""); // Test that successive updates are working
        });
  }

  @Test
  void canRollbackChangelog() {
    runLiquibase(
        "changelog.xml",
        (liquibase, database) -> {
          liquibase.update("");
          liquibase.rollback(2, "");
        });
  }

  @Test
  void canTagDatabase() {
    runLiquibase(
        "changelog.xml",
        (liquibase, database) -> {
          liquibase.update("");
          liquibase.tag("testTag");
        });
  }

  @Test
  void canValidate() {
    runLiquibase("changelog.xml", (liquibase, database) -> liquibase.validate());
  }

  @Test
  void canListLocks() {
    runLiquibase("changelog.xml", (liquibase, database) -> liquibase.listLocks());
  }

  @Test
  void canSyncChangelog() {
    runLiquibase("changelog.xml", (liquibase, database) -> liquibase.changeLogSync(""));
  }

  @Test
  void canForceReleaseLocks() {
    runLiquibase("changelog.xml", (liquibase, database) -> liquibase.forceReleaseLocks());
  }

  @Test
  void canReportStatus() {
    runLiquibase(
        "changelog.xml",
        (liquibase, database) -> liquibase.reportStatus(true, "", new NullWriter()));
  }

  @Test
  void canMarkChangeSetRan() {
    runLiquibase("changelog.xml", (liquibase, database) -> liquibase.markNextChangeSetRan(""));
  }

  @Test
  void usesComClickHouseDriver() {
    runLiquibase(
        "empty-changelog.xml",
        (liquibase, database) ->
            assertTrue(
                ((JdbcConnection) database.getConnection())
                    .getUnderlyingConnection()
                    .getClass()
                    .getName()
                    .startsWith("com.clickhouse.")));
  }

  // Changesets with rollbackSplitStatements:false are not split by Liquibase (its splitStatements
  // pattern matches that attribute too), so the whole script reaches the extension at once.
  @Test
  void canExecuteMultiStatementChangeset() {
    runLiquibase(
        "multi_update",
        "multi-statement-changelog.xml",
        (liquibase, database) -> {
          liquibase.update("");
          assertEquals(
              "semi;colon|escaped ' quote",
              queryString(
                  database,
                  "SELECT arrayStringConcat(groupArray(label), '|')"
                      + " FROM (SELECT label FROM ms_events ORDER BY id)"));
          assertEquals(
              "MaterializedView",
              queryString(
                  database,
                  "SELECT engine FROM system.tables WHERE database = 'multi_update'"
                      + " AND name = 'ms_events_mv'"));
        });
  }

  @Test
  void canRollbackMultiStatementChangeset() {
    runLiquibase(
        "multi_rollback",
        "multi-statement-changelog.xml",
        (liquibase, database) -> {
          liquibase.update("");
          liquibase.rollback(1, "");
          assertEquals(
              "0",
              queryString(
                  database,
                  "SELECT count() FROM system.tables WHERE database = 'multi_rollback'"
                      + " AND name LIKE 'ms_events%'"));
        });
  }

  private String queryString(Database database, String sql) throws Exception {
    try (Statement statement =
            ((JdbcConnection) database.getConnection())
                .getUnderlyingConnection()
                .createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      resultSet.next();
      return resultSet.getString(1);
    }
  }

  private void runLiquibase(
      String changelog, ThrowingBiConsumer<Liquibase, Database> liquibaseAction) {
    runLiquibase(null, changelog, liquibaseAction);
  }

  // A non-null databaseName runs against a fresh database given in the JDBC URL instead of default
  private void runLiquibase(
      String databaseName,
      String changelog,
      ThrowingBiConsumer<Liquibase, Database> liquibaseAction) {
    DatabaseFactory dbFactory = DatabaseFactory.getInstance();
    ResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor();

    try {
      Connection connection = clickHouseContainer.createConnection("");
      if (databaseName != null) {
        try (Statement statement = connection.createStatement()) {
          statement.execute("DROP DATABASE IF EXISTS " + databaseName);
          statement.execute("CREATE DATABASE " + databaseName);
        }
        connection.close();
        connection =
            DriverManager.getConnection(
                clickHouseContainer.getJdbcUrl().replaceFirst("/default$", "/" + databaseName),
                clickHouseContainer.getUsername(),
                clickHouseContainer.getPassword());
      }
      JdbcConnection jdbcConnection = new JdbcConnection(connection);
      Database database = dbFactory.findCorrectDatabaseImplementation(jdbcConnection);
      Liquibase liquibase = new Liquibase(changelog, resourceAccessor, database);
      liquibaseAction.accept(liquibase, database);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @FunctionalInterface
  private interface ThrowingBiConsumer<T1, T2> {
    void accept(T1 t1, T2 t2) throws java.lang.Exception;
  }
}
