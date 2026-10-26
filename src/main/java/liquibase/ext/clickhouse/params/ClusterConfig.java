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
package liquibase.ext.clickhouse.params;

public class ClusterConfig {
  private String clusterName;
  private String tableZooKeeperPathPrefix;
  private String tableReplicaName;

  public ClusterConfig() {}

  public ClusterConfig(
      String clusterName, String tableZooKeeperPathPrefix, String tableReplicaName) {
    this.clusterName = clusterName;
    this.tableZooKeeperPathPrefix = tableZooKeeperPathPrefix;
    this.tableReplicaName = tableReplicaName;
  }

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public String getTableZooKeeperPathPrefix() {
    return tableZooKeeperPathPrefix;
  }

  public void setTableZooKeeperPathPrefix(String tableZooKeeperPathPrefix) {
    this.tableZooKeeperPathPrefix = tableZooKeeperPathPrefix;
  }

  public String getTableReplicaName() {
    return tableReplicaName;
  }

  public void setTableReplicaName(String tableReplicaName) {
    this.tableReplicaName = tableReplicaName;
  }
}
