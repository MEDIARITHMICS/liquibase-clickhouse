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

import java.sql.Connection;

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
      new ClickHouseContainer("clickhouse/clickhouse-server:22.3.8.39");

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

  private void runLiquibase(
      String changelog, ThrowingBiConsumer<Liquibase, Database> liquibaseAction) {
    DatabaseFactory dbFactory = DatabaseFactory.getInstance();
    ResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor();

    try {
      Connection connection = clickHouseContainer.createConnection("");
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
