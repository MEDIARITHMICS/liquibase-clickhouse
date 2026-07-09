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

import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.ResourceAccessor;
import org.junit.jupiter.api.Test;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.StringWriter;
import java.sql.Connection;

@Testcontainers
public class ClickHouseTest {

  private static final DockerImageName clickhouseImage = DockerImageName.parse("clickhouse/clickhouse-server");

  @Container
  private static final ClickHouseContainer clickHouseContainer = new ClickHouseContainer(clickhouseImage)
      .withUsername("test-clickhouse-username")
      .withPassword("test-clickhouse-password");

  @Test
  void canInitializeLiquibaseSchema() throws Exception {
    runLiquibase("empty-changelog.xml", (liquibase, database) -> liquibase.update(""));
  }

  @Test
  void canExecuteChangelog() throws Exception {
    runLiquibase(
        "changelog.xml",
        (liquibase, database) -> {
          liquibase.update("");
          liquibase.update(""); // Test that successive updates are working
        });
  }

  @Test
  void canRollbackChangelog() throws Exception {
    runLiquibase(
        "changelog.xml",
        (liquibase, database) -> {
          liquibase.update("");
          liquibase.rollback(2, "");
        });
  }

  @Test
  void canTagDatabase() throws Exception {
    runLiquibase(
        "changelog.xml",
        (liquibase, database) -> {
          liquibase.update("");
          liquibase.tag("testTag");
        });
  }

  @Test
  void canValidate() throws Exception {
    runLiquibase("changelog.xml", (liquibase, database) -> liquibase.validate());
  }

  @Test
  void canListLocks() throws Exception {
    runLiquibase("changelog.xml", (liquibase, database) -> liquibase.listLocks());
  }

  @Test
  void canSyncChangelog() throws Exception {
    runLiquibase("changelog.xml", (liquibase, database) -> liquibase.changeLogSync(""));
  }

  @Test
  void canForceReleaseLocks() throws Exception {
    runLiquibase("changelog.xml", (liquibase, database) -> liquibase.forceReleaseLocks());
  }

  @Test
  void canReportStatus() throws Exception {
    runLiquibase(
        "changelog.xml",
        (liquibase, database) -> liquibase.reportStatus(true, "", new StringWriter()));
  }

  @Test
  void canMarkChangeSetRan() throws Exception {
    runLiquibase("changelog.xml", (liquibase, database) -> liquibase.markNextChangeSetRan(""));
  }

  private void runLiquibase(String changelog, ThrowingBiConsumer<Liquibase, Database> liquibaseAction) throws Exception {
    DatabaseFactory dbFactory = DatabaseFactory.getInstance();
    ResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor();

    Connection connection = clickHouseContainer.createConnection("");
    JdbcConnection jdbcConnection = new JdbcConnection(connection);
    Database database = dbFactory.findCorrectDatabaseImplementation(jdbcConnection);

    try(Liquibase liquibase = new Liquibase(changelog, resourceAccessor, database)) {
      liquibaseAction.accept(liquibase, database);
    }
  }

  @FunctionalInterface
  private interface ThrowingBiConsumer<T1, T2> {
    void accept(T1 t1, T2 t2) throws java.lang.Exception;
  }
}
