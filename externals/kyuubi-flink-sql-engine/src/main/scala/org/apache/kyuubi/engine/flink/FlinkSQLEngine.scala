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

import org.apache.flink.api.java.{ExecutionEnvironment, LocalEnvironment, RemoteEnvironment}

import java.util.concurrent.CountDownLatch
import org.apache.kyuubi.Logging
import org.apache.kyuubi.Utils.{FLINK_ENGINE_SHUTDOWN_PRIORITY, addShutdownHook}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.flink.FlinkSQLEngine.countDownLatch
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ZK_ENGINE_REF_ID
import org.apache.kyuubi.service.Serverable
import org.apache.kyuubi.util.SignalRegister

/**
 * A flink sql engine just like an instance of Flink SQL Gateway.
 */
case class FlinkSQLEngine(env: ExecutionEnvironment) extends Serverable("FlinkSQLEngine") {

  override val backendService = new FlinkSQLBackendService(env)
  override val frontendServices = Seq(new FlinkThriftBinaryFrontendService(this))

  override def initialize(conf: KyuubiConf): Unit = super.initialize(conf)

  override protected def stopServer(): Unit = {
    countDownLatch.countDown()
  }

  override def start(): Unit = {
    super.start()

    backendService.sessionManager.startTerminatingChecker()
  }

  override def stop(): Unit = {
    super.stop()
  }

}

object FlinkSQLEngine extends Logging {

  val kyuubiConf: KyuubiConf = KyuubiConf()

  var currentEngine: Option[FlinkSQLEngine] = None

  private val countDownLatch = new CountDownLatch(1)

  def main(args: Array[String]): Unit = {
    SignalRegister.registerLogger(logger)

    try {
      logger.info(System.getProperties.toString)
      logger.info(System.getProperty("kyuubi.ha.engine.ref.id"))
      kyuubiConf.set(HA_ZK_ENGINE_REF_ID, System.getProperty("kyuubi.ha.engine.ref.id"))

      var env = createExecutionEnvironment()
      startEngine(env)

      // blocking main thread
      countDownLatch.await()
    } catch {
      case t: Throwable if currentEngine.isDefined =>
        currentEngine.foreach { engine =>
          error(t)
          engine.stop()
        }
      case t: Throwable =>
        error("Create FlinkSQL Engine Failed", t)
    }
  }

  def startEngine(env: ExecutionEnvironment): Unit = {
    currentEngine = Some(new FlinkSQLEngine(env))
    currentEngine.foreach { engine =>
      engine.initialize(kyuubiConf)
      engine.start()
      addShutdownHook(() => engine.stop(), FLINK_ENGINE_SHUTDOWN_PRIORITY + 1)
    }
  }

  def createExecutionEnvironment(): ExecutionEnvironment = {
    kyuubiConf.setIfMissing(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    val env = ExecutionEnvironment.getExecutionEnvironment
    if (env.isInstanceOf[LocalEnvironment]) {
      logger.info("local env")
    } else if (env.isInstanceOf[RemoteEnvironment]) {
      logger.info("remote env")
    } else {
      logger.info("other env")
    }
    env
  }
}
