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
package liquibase.ext.clickhouse.sqlgenerator;

import liquibase.ext.clickhouse.database.ClickHouseDatabase;
import liquibase.ext.clickhouse.params.ClusterConfig;
import liquibase.ext.clickhouse.params.ParamsLoader;

import liquibase.ContextExpression;
import liquibase.change.Change;
import liquibase.change.core.TagDatabaseChange;
import liquibase.changelog.ChangeLogHistoryServiceFactory;
import liquibase.changelog.ChangeSet;
import liquibase.database.Database;
import liquibase.database.ObjectQuotingStrategy;
import liquibase.exception.LiquibaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.MarkChangeSetRanGenerator;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.core.MarkChangeSetRanStatement;
import liquibase.util.StringUtil;

public class MarkChangeSetRanGeneratorClickHouse extends MarkChangeSetRanGenerator {
  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }

  @Override
  public boolean supports(MarkChangeSetRanStatement statement, Database database) {
    return database instanceof ClickHouseDatabase;
  }

  @Override
  public Sql[] generateSql(
      MarkChangeSetRanStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
    String dateValue = database.getCurrentDateTimeFunction();

    ChangeSet changeSet = statement.getChangeSet();

    // use LEGACY quoting since we're dealing with system objects
    ObjectQuotingStrategy currentStrategy = database.getObjectQuotingStrategy();
    database.setObjectQuotingStrategy(ObjectQuotingStrategy.LEGACY);
    try {
      try {
        if (statement.getExecType().equals(ChangeSet.ExecType.FAILED)
            || statement.getExecType().equals(ChangeSet.ExecType.SKIPPED)) {
          return new Sql[0]; // don't mark
        }

        String tag = null;
        for (Change change : changeSet.getChanges()) {
          if (change instanceof TagDatabaseChange) {
            TagDatabaseChange tagChange = (TagDatabaseChange) change;
            tag = tagChange.getTag();
          }
        }

        if (statement.getExecType().ranBefore) {
          ClusterConfig properties = ParamsLoader.getLiquibaseClickhouseProperties();
          String updateQuery =
              String.format(
                  "ALTER TABLE %s.%s "
                      + SqlGeneratorUtil.generateSqlOnClusterClause(properties)
                      + "UPDATE COMMENTS = '%s', CONTEXTS = '%s', DATEEXECUTED = %s, DEPLOYMENT_ID = '%s', EXECTYPE = '%s', LABELS = '%s', MD5SUM = '%s', ORDEREXECUTED = %d "
                      + (tag != null ? ", TAG = '%s'" : "%s")
                      + " WHERE ID = '%s' AND AUTHOR = '%s' AND FILENAME = '%s'",
                  database.getDefaultSchemaName(),
                  database.getDatabaseChangeLogTableName(),
                  getCommentsColumn(changeSet),
                  getContextsColumn(changeSet),
                  new DatabaseFunction(dateValue),
                  ChangeLogHistoryServiceFactory.getInstance()
                      .getChangeLogService(database)
                      .getDeploymentId(),
                  statement.getExecType().value,
                  getLabelsColumn(changeSet),
                  changeSet.generateCheckSum().toString(),
                  ChangeLogHistoryServiceFactory.getInstance()
                      .getChangeLogService(database)
                      .getNextSequenceValue(),
                  tag != null ? tag : "",
                  // WhereClause
                  changeSet.getId(),
                  changeSet.getAuthor(),
                  changeSet.getFilePath());
          return SqlGeneratorUtil.generateSql(database, updateQuery);
        } else {
          return super.generateSql(statement, database, sqlGeneratorChain);
        }
      } catch (LiquibaseException e) {
        throw new UnexpectedLiquibaseException(e);
      }

    } finally {
      database.setObjectQuotingStrategy(currentStrategy);
    }
  }

  private String getCommentsColumn(ChangeSet changeSet) {
    return limitSize(StringUtil.trimToEmpty(changeSet.getComments()));
  }

  protected String getContextsColumn(ChangeSet changeSet) {
    return changeSet.getContexts() != null && !changeSet.getContexts().isEmpty()
        ? buildFullContext(changeSet)
        : null;
  }

  private String getLabelsColumn(ChangeSet changeSet) {
    return changeSet.getLabels() != null && !changeSet.getLabels().isEmpty()
        ? changeSet.getLabels().toString()
        : null;
  }

  private String limitSize(String string) {
    int maxLength = 250;
    if (string.length() > maxLength) {
      return string.substring(0, maxLength - 3) + "...";
    }
    return string;
  }

  /**
   * Build and return a string which contains both the changeset and inherited context
   *
   * @return String
   */
  private String buildFullContext(ChangeSet changeSet) {
    StringBuilder contextExpression = new StringBuilder();
    boolean notFirstContext = false;
    for (ContextExpression inheritableContext : changeSet.getInheritableContexts()) {
      this.appendContext(contextExpression, inheritableContext.toString(), notFirstContext);
    }

    ContextExpression changeSetContext = changeSet.getContexts();
    if (changeSetContext != null && !changeSetContext.isEmpty()) {
      appendContext(contextExpression, changeSetContext.toString(), notFirstContext);
    }

    return StringUtil.trimToNull(contextExpression.toString());
  }

  private void appendContext(
      StringBuilder contextExpression, String contextToAppend, boolean notFirstContext) {
    boolean complexExpression = contextToAppend.contains(",") || contextToAppend.contains(" ");
    if (notFirstContext) {
      contextExpression.append(" AND ");
    }

    if (complexExpression) {
      contextExpression.append("(");
    }

    contextExpression.append(contextToAppend);
    if (complexExpression) {
      contextExpression.append(")");
    }
  }
}
