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

package org.apache.kyuubi.engine.flink.session

import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.hive.service.rpc.thrift.TProtocolVersion
import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf.ENGINE_SHARE_LEVEL
import org.apache.kyuubi.engine.ShareLevel
import org.apache.kyuubi.engine.flink.FlinkSQLEngine
import org.apache.kyuubi.engine.flink.operation.FlinkSQLOperationManager
import org.apache.kyuubi.session.{SessionHandle, SessionManager}

class FlinkSQLSessionManager(env: ExecutionEnvironment)
  extends SessionManager("FlinkSQLSessionManager") {

  override protected def isServer: Boolean = false

  val operationManager = new FlinkSQLOperationManager

  override def openSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Predef.Map[String, String]): SessionHandle = {
    info(s"Opening session for $user@$ipAddress")

    info(s"config is : ${conf.seq.toString()}")

    val sessionImpl = new FlinkSessionImpl(protocol, user, password, ipAddress, conf, this, null)
    val handle = sessionImpl.handle

//    sessionImpl.normalizedConf.foreach {
//
//    }

    try {
      sessionImpl.open()

      operationManager.setFlinkSession(handle, env)
      setSession(handle, sessionImpl)
      info(s"$user's session with $handle is opened, current opening sessions" +
        s" $getOpenSessionCount")
      handle
    } catch {
      case e: Exception =>
        sessionImpl.close()
        throw KyuubiSQLException(e)
    }

//    SessionHandle(HandleIdentifier(), TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1)

//    val defaultContext = new DefaultContext()
//
//    val executionType = ExecutionEntry.EXECUTION_TYPE_VALUE_BATCH
//
//    // build session context
//    val newProperties: util.Map[String, String] = new util.HashMap[String, String]()
//    newProperties.put(Environment.EXECUTION_ENTRY + "." + ExecutionEntry.EXECUTION_PLANNER, null)
//    newProperties.put(Environment.EXECUTION_ENTRY + "." + ExecutionEntry.EXECUTION_TYPE,
//      executionType)
//
//    if (executionType.equalsIgnoreCase(ExecutionEntry.EXECUTION_TYPE_VALUE_BATCH)) {
//      // for batch mode we ensure that results are provided in materialized form
//      newProperties.put(Environment.EXECUTION_ENTRY + "." + ExecutionEntry.EXECUTION_RESULT_MODE,
//        ExecutionEntry.EXECUTION_RESULT_MODE_VALUE_TABLE)
//    }
//    else { // for streaming mode we ensure that results are provided in changelog form
//      newProperties.put(Environment.EXECUTION_ENTRY + "." + ExecutionEntry.EXECUTION_RESULT_MODE,
//        ExecutionEntry.EXECUTION_RESULT_MODE_VALUE_CHANGELOG)
//    }
//
//    val sessionEnv: Environment = Environment.enrich(defaultContext.getDefaultEnv,
//      newProperties, Collections.emptyMap)
//
//    val sessionContext = new SessionContext("default session", sessionEnv, defaultContext)
//
//    val session = new FlinkSessionImpl(protocol, user, password, ipAddress, conf, this, sessionContext)
//    val handle = session.handle
//    // flink sql gateway SessionManager#createSession
//
//    try {
//
//      session.open()
//
//      setSession(handle, session)
//      info(s"$user's session with $handle is opened, current opening sessions" +
//        s" $getOpenSessionCount")
//      handle
//    } catch {
//      case e: Exception =>
//        session.close()
//        throw KyuubiSQLException(e)
//    }
  }

  override def closeSession(sessionHandle: SessionHandle): Unit = {
    super.closeSession(sessionHandle)
    operationManager.removeFlinkSession(sessionHandle)
    if (conf.get(ENGINE_SHARE_LEVEL) == ShareLevel.CONNECTION.toString) {
      info("Session stopped due to shared level is Connection.")
      stopSession()
    }
  }

  private def stopSession(): Unit = {
    FlinkSQLEngine.currentEngine.foreach(_.stop())
  }
}
