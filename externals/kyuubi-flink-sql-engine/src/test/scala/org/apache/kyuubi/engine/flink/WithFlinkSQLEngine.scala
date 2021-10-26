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

package org.apache.kyuubi.engine.flink

import org.apache.flink.api.java.ExecutionEnvironment

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf

trait WithFlinkSQLEngine extends KyuubiFunSuite {

  protected var env: ExecutionEnvironment = _
  protected var engine: FlinkSQLEngine = _
  // conf will be loaded until start spark engine
  def withKyuubiConf: Map[String, String]
  val kyuubiConf: KyuubiConf = FlinkSQLEngine.kyuubiConf

  protected var connectionUrl: String = _

  override def beforeAll(): Unit = {
    startFlinkEngine()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    stopFlinkEngine()
  }

  def startFlinkEngine(): Unit = {
    withKyuubiConf.foreach { case (k, v) =>
      System.setProperty(k, v)
      kyuubiConf.set(k, v)
    }
    env = FlinkSQLEngine.createExecutionEnvironment()
    FlinkSQLEngine.startEngine(env)
    engine = FlinkSQLEngine.currentEngine.get
    connectionUrl = engine.frontendServices.head.connectionUrl
  }

  def stopFlinkEngine(): Unit = {
    if (engine != null) {
      engine.stop()
      engine = null
    }

    if (env != null) {
      env = null
    }
  }

  protected def getJdbcUrl: String = s"jdbc:hive2://$connectionUrl/;"
  def getFlinkEnv: ExecutionEnvironment = env

}
