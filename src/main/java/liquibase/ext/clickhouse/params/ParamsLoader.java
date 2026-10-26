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

import java.io.*;
import java.util.*;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import liquibase.Scope;
import liquibase.logging.Logger;

public class ParamsLoader {
  private static final Logger LOG = Scope.getCurrentScope().getLog(ParamsLoader.class);

  private static ClusterConfig liquibaseClickhouseProperties = null;

  private static Set<String> validProperties =
      new HashSet<>(Arrays.asList("clusterName", "tableZooKeeperPathPrefix", "tableReplicaName"));

  private static StringBuilder appendWithComma(StringBuilder sb, String text) {
    if (sb.length() > 0) sb.append(", ");
    sb.append(text);

    return sb;
  }

  private static String getMissingProperties(Set<String> properties) {
    StringBuilder missingProperties = new StringBuilder();
    for (String validProperty : validProperties)
      if (!properties.contains(validProperty)) appendWithComma(missingProperties, validProperty);

    return missingProperties.toString();
  }

  private static void checkProperties(Map<String, String> properties)
      throws InvalidPropertiesFormatException {
    StringBuilder errMsg = new StringBuilder();

    for (String key : properties.keySet())
      if (!validProperties.contains(key)) {
        appendWithComma(errMsg, "unknown property: ").append(key);
      }

    if (errMsg.length() > 0 || properties.size() != validProperties.size()) {
      appendWithComma(errMsg, "the missing properties should be defined: ");
      errMsg.append(getMissingProperties(properties.keySet()));
    }

    if (errMsg.length() > 0) throw new InvalidPropertiesFormatException(errMsg.toString());
  }

  private static String getStackTrace(Exception e) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    e.printStackTrace(pw);

    return sw.toString();
  }

  public static ClusterConfig getLiquibaseClickhouseProperties() {
    return getLiquibaseClickhouseProperties("liquibaseClickhouse");
  }

  public static ClusterConfig getLiquibaseClickhouseProperties(String configFile) {
    if (liquibaseClickhouseProperties != null) return liquibaseClickhouseProperties;

    Config conf = ConfigFactory.load(configFile);
    Map<String, String> params = new HashMap<>();
    ClusterConfig result = null;

    try {
      for (Map.Entry<String, ConfigValue> s : conf.getConfig("cluster").entrySet())
        params.put(s.getKey(), s.getValue().unwrapped().toString());

      checkProperties(params);
      result =
          new ClusterConfig(
              params.get("clusterName"),
              params.get("tableZooKeeperPathPrefix"),
              params.get("tableReplicaName"));

      LOG.info(
          "Cluster settings ("
              + configFile
              + ".conf) are found. Work in cluster replicated clickhouse mode.");
    } catch (ConfigException.Missing e) {
      LOG.info(
          "Cluster settings ("
              + configFile
              + ".conf) are not defined. Work in single-instance clickhouse mode.");
      LOG.info(
          "The following properties should be defined: " + getMissingProperties(new HashSet<>()));
    } catch (InvalidPropertiesFormatException e) {
      LOG.severe(getStackTrace(e));
      LOG.severe("Work in single-instance clickhouse mode.");
    }

    return result;
  }
}
