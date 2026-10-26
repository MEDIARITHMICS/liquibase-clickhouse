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
package liquibase;

import liquibase.ext.clickhouse.params.ClusterConfig;
import liquibase.ext.clickhouse.params.ParamsLoader;

import org.junit.jupiter.api.Test;

public class ParamsLoaderTest {

  @Test
  void loadParams() {
    ClusterConfig params = ParamsLoader.getLiquibaseClickhouseProperties("testLiquibaseClickhouse");
    assert (params != null);
    assert (params.getClusterName().equals("Cluster1"));
    assert (params.getTableZooKeeperPathPrefix().equals("Path1"));
    assert (params.getTableReplicaName().equals("Replica1"));
  }

  @Test
  void loadBrokenParams() {
    ClusterConfig params =
        ParamsLoader.getLiquibaseClickhouseProperties("testLiquibaseClickhouseBroken");
    assert (params == null);
  }
}
