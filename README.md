# liquibase-clickhouse [![maven][maven-image]][maven-url]
                       
[maven-image]: https://img.shields.io/maven-central/v/com.mediarithmics/liquibase-clickhouse.svg?maxAge=259200&style=for-the-badge&color=brithgreen&label=com.mediarithmics:liquibase-clickhouse
[maven-url]: https://search.maven.org/search?q=a:liquibase-clickhouse

Maven dependency:

```
<dependency>
    <groupId>com.mediarithmics</groupId>
    <artifactId>liquibase-clickhouse</artifactId>
    <version>Latest version from Maven Central</version>
</dependency>
```

The cluster mode can be activated by adding the **_liquibaseClickhouse.conf_** file to the classpath (liquibase/lib/).
```
cluster {
    clusterName="{cluster}"
    tableZooKeeperPathPrefix="/clickhouse/tables/{shard}/{database}/"
    tableReplicaName="{replica}"
}
```
In this mode, liquibase will create its own tables as replicated.<br/>
All changes in these files will be replicated on the entire cluster.<br/>
Your updates should also affect the entire cluster either by using ON CLUSTER clause, or by using replicated tables.

<hr/>

###### Important changes
From the version 0.7.1 the liquibase-clickhouse supports replication on a cluster. 

From the version 0.6.0 the extension adapted for the liquibase v4.3.5.

