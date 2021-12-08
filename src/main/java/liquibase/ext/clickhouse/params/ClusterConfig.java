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
