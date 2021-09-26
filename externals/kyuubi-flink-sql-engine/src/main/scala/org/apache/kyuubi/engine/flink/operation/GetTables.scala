/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.engine.flink.operation

import java.util
import java.util.Collections

import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.table.types.logical.VarCharType
import org.apache.flink.types.Row

import org.apache.kyuubi.engine.flink.operation.FlinkOperation.toJavaRegex
import org.apache.kyuubi.engine.flink.result.{ColumnInfo, ResultSet}
import org.apache.kyuubi.operation.OperationType
import org.apache.kyuubi.session.Session

class GetTables(
    env: ExecutionEnvironment,
    session: Session,
    catalog: String,
    schema: String,
    tableName: String,
    tableTypes: Set[String])
  extends FlinkOperation(env, OperationType.GET_TABLES, session) {

  override protected def runInternal(): Unit = {
    val schemaPattern = toJavaRegex(schema)
    val tablePattern = toJavaRegex(tableName)

    // every Flink database belongs to a catalog and a database
    if ("" == catalog || "" == schemaPattern) {
      resultSet = ResultSet.builder()
        .columns(new GetTableResultColumnInfos().getColumnInfos)
        .data(Collections.emptyList[Row]())
        .build()
    }

    resultSet = null
  }
}

/**
 * Candidate result for table related interface.
 */
class TableResultData private (
    val catalog: String,
    val database: String,
    val table: String,
    val `type`: String) {

}

class GetTableResultColumnInfos {

  private var maxCatalogNameLength = 1
  private var maxDatabaseNameLength = 1
  private var maxTableNameLength = 1

  def process(data: TableResultData): Row = {
    maxCatalogNameLength = Math.max(maxCatalogNameLength, data.catalog.length)
    maxDatabaseNameLength = Math.max(maxDatabaseNameLength, data.database.length)
    maxTableNameLength = Math.max(maxTableNameLength, data.table.length)
    Row.of(data.catalog, data.database, data.table, data.`type`, null, null, null, null, null, null)
  }

  // according to the java doc of DatabaseMetaData#getTables
  def getColumnInfos: util.List[ColumnInfo] = {
    util.Arrays.asList(
      ColumnInfo.create("TABLE_CAT", new VarCharType(true, maxCatalogNameLength)),
      ColumnInfo.create("TABLE_SCHEM", new VarCharType(true, maxDatabaseNameLength)),
      // currently can only be TABLE or VIEW
      ColumnInfo.create("TABLE_NAME", new VarCharType(false, maxTableNameLength)),
      // currently these columns are null
      ColumnInfo.create("TABLE_TYPE", new VarCharType(false, 5)),
      ColumnInfo.create("REMARKS", new VarCharType(true, 1)),
      ColumnInfo.create("TYPE_CAT", new VarCharType(true, 1)),
      ColumnInfo.create("TYPE_SCHEM", new VarCharType(true, 1)),
      ColumnInfo.create("TYPE_NAME", new VarCharType(true, 1)),
      ColumnInfo.create("SELF_REFERENCING_COL_NAME", new VarCharType(true, 1)),
      ColumnInfo.create("REF_GENERATION", new VarCharType(true, 1)))
  }
}

object MetaInfoUtils {

  def getSingleResult(stmt: String, env: ExecutionEnvironment): ResultSet = {
    null
  }

}