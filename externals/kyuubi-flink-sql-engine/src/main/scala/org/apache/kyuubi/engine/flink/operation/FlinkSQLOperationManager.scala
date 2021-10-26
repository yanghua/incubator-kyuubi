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

import org.apache.flink.api.java.ExecutionEnvironment

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.operation.{Operation, OperationManager}
import org.apache.kyuubi.session.{Session, SessionHandle}

import java.util
import java.util.concurrent.ConcurrentHashMap

class FlinkSQLOperationManager extends OperationManager("FlinkSQLOperationManager") {

  private val sessionToFlink = new ConcurrentHashMap[SessionHandle, ExecutionEnvironment]()

  def getFlinkSession(sessionHandle: SessionHandle): ExecutionEnvironment = {
    logger.info(s"Invoke getFlinkSession: $sessionHandle")
    logger.info(sessionToFlink.toString)
    val flinkSession = sessionToFlink.get(sessionHandle)
    if (flinkSession == null) {
      throw KyuubiSQLException(s"$sessionHandle has not been initialized or already been closed")
    }
    flinkSession
  }

  def setFlinkSession(sessionHandle: SessionHandle, env: ExecutionEnvironment): Unit = {
    logger.info(s"Invoke setFlinkSession: $sessionHandle")
    sessionToFlink.put(sessionHandle, env)
    logger.info(sessionToFlink.toString)
  }

  def removeFlinkSession(sessionHandle: SessionHandle): ExecutionEnvironment = {
    logger.info(s"Invoke removeFlinkSession: $sessionHandle")
    logger.info(sessionToFlink.toString)
    sessionToFlink.remove(sessionHandle)
  }

  override def newExecuteStatementOperation(
      session: Session,
      statement: String,
      runAsync: Boolean,
      queryTimeout: Long): Operation = null

  override def newGetTypeInfoOperation(session: Session): Operation = {
    info("Invoke newGetTypeInfoOperation ")
    null
  }

  override def newGetCatalogsOperation(session: Session): Operation = {
    info(s"Invoke newGetCatalogsOperation: ${session.handle}")
    val env = getFlinkSession(session.handle)
    val op = new GetCatalogs(env, session)
    addOperation(op)
  }

  override def newGetSchemasOperation(
      session: Session,
      catalog: String,
      schema: String): Operation = null

  override def newGetTablesOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: util.List[String]): Operation = {
    info("Invoke newGetTablesOperation")
    null
  }

  override def newGetTableTypesOperation(session: Session): Operation = {
    val env = getFlinkSession(session.handle)
    val op = new GetTableTypes(env, session)
    addOperation(op)
  }

  override def newGetColumnsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): Operation = null

  override def newGetFunctionsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      functionName: String): Operation = null

}
