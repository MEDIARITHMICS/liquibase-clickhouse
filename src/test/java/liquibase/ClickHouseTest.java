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
package liquibase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.TimeUnit;

import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.ResourceAccessor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.apache.commons.io.output.NullWriter;
import org.testcontainers.utility.MountableFile;

@Testcontainers
public class ClickHouseTest {

  @Container
  private static final GenericContainer<?> CONTAINER =
      new GenericContainer<>("clickhouse/clickhouse-server:26.1")
          .withExposedPorts(8123)
          .withCopyFileToContainer(
              MountableFile.forClasspathResource("users.xml"),
              "/etc/clickhouse-server/users.d/users.xml")
          .waitingFor(Wait.forHttp("/ping").forPort(8123));

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void canInitializeLiquibaseSchema() {
    runLiquibase("empty-changelog.xml", (liquibase, database) -> {
      liquibase.update("");
    });
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void canExecuteChangelog() {
    runLiquibase(
        "changelog.xml",
        (liquibase, database) -> {
          liquibase.update("");
          liquibase.update(""); // Test that successive updates are working
        });
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void canRollbackChangelog() {
    runLiquibase(
        "changelog.xml",
        (liquibase, database) -> {
          liquibase.update("");
          liquibase.rollback(2, "");
        });
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void canTagDatabase() {
    runLiquibase(
        "changelog.xml",
        (liquibase, database) -> {
          liquibase.update("");
          liquibase.tag("testTag");
        });
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void canValidate() {
    runLiquibase("changelog.xml", (liquibase, database) -> {
      liquibase.validate();
    });
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void canListLocks() {
    runLiquibase("changelog.xml", (liquibase, database) -> {
      liquibase.listLocks();
    });
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void canSyncChangelog() {
    runLiquibase("changelog.xml", (liquibase, database) -> {
      liquibase.changeLogSync("");
    });
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void canForceReleaseLocks() {
    runLiquibase("changelog.xml", (liquibase, database) -> {
      liquibase.forceReleaseLocks();
    });
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void canReportStatus() {
    runLiquibase(
        "changelog.xml",
        (liquibase, database) -> {
          liquibase.reportStatus(true, "", new NullWriter());
        });
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void canMarkChangeSetRan() {
    runLiquibase("changelog.xml", (liquibase, database) -> {
      liquibase.markNextChangeSetRan("");
    });
  }

  private void runLiquibase(
      String changelog, ThrowingBiConsumer<Liquibase, Database> liquibaseAction) {
    DatabaseFactory dbFactory = DatabaseFactory.getInstance();
    ResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor();

    String url = String.format("jdbc:clickhouse://%s:%d/default?user=test_user&password=test_password", CONTAINER.getHost(), CONTAINER.getMappedPort(8123));
    try (Connection connection = DriverManager.getConnection(url)) {
      JdbcConnection jdbcConnection = new JdbcConnection(connection);
      Database database = dbFactory.findCorrectDatabaseImplementation(jdbcConnection);
      database.setDefaultSchemaName("default");
      // Use RawSqlStatement directly to avoid any database-specific locking logic during cleanup
      connection.createStatement().execute("DROP TABLE IF EXISTS DATABASECHANGELOGLOCK");
      connection.createStatement().execute("DROP TABLE IF EXISTS DATABASECHANGELOG");
      connection.createStatement().execute("DROP TABLE IF EXISTS test");
      try (Liquibase liquibase = new Liquibase(changelog, resourceAccessor, database)) {
        liquibaseAction.accept(liquibase, database);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @FunctionalInterface
  private interface ThrowingBiConsumer<T1, T2> {
    void accept(T1 t1, T2 t2) throws java.lang.Exception;
  }
}
