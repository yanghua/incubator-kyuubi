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

import java.io.File
import java.net.URL
import java.util
import java.util.concurrent.CountDownLatch

import scala.collection.JavaConversions._

import org.apache.flink.util.JarUtils

import org.apache.kyuubi.Logging
import org.apache.kyuubi.Utils.{FLINK_ENGINE_SHUTDOWN_PRIORITY, addShutdownHook}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.flink.FlinkSQLEngine.countDownLatch
import org.apache.kyuubi.engine.flink.config.EngineEnvironment
import org.apache.kyuubi.engine.flink.config.EnvironmentUtil.readEnvironment
import org.apache.kyuubi.engine.flink.context.EngineContext
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ZK_ENGINE_REF_ID
import org.apache.kyuubi.service.Serverable
import org.apache.kyuubi.util.SignalRegister

/**
 * A flink sql engine just like an instance of Flink SQL Gateway.
 */
case class FlinkSQLEngine(engineContext: EngineContext) extends Serverable("FlinkSQLEngine") {

  override val backendService = new FlinkSQLBackendService(engineContext)
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
      logger.info(System.getProperty("kyuubi.ha.engine.ref.id"))
      kyuubiConf.set(HA_ZK_ENGINE_REF_ID, System.getProperty("kyuubi.ha.engine.ref.id"))

      val engineEnv = createEngineEnvironment()
      val dependencies = FlinkSQLEngine.discoverDependencies(null, null)
      val defaultContext = new EngineContext(engineEnv, dependencies)

      startEngine(defaultContext)

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

  def startEngine(engineContext: EngineContext): Unit = {
    currentEngine = Some(new FlinkSQLEngine(engineContext))
    currentEngine.foreach { engine =>
      engine.initialize(kyuubiConf)
      engine.start()
      addShutdownHook(() => engine.stop(), FLINK_ENGINE_SHUTDOWN_PRIORITY + 1)
    }
  }

  def createEngineEnvironment(): EngineEnvironment = {
    kyuubiConf.setIfMissing(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    // TODO TBD
    readEnvironment(null)
  }

  def discoverDependencies(jars: util.List[URL], libraries: util.List[URL]): util.List[URL] = {
    val dependencies = new util.ArrayList[URL]
    try { // find jar files
      for (url <- jars) {
        JarUtils.checkJarFile(url)
        dependencies.add(url)
      }
      // find jar files in library directories
      for (libUrl <- libraries) {
        val dir = new File(libUrl.toURI)
        if (!dir.isDirectory) throw new RuntimeException("Directory expected: " + dir)
        else if (!dir.canRead) throw new RuntimeException("Directory cannot be read: " + dir)
        val files = dir.listFiles
        if (files == null) throw new RuntimeException("Directory cannot be read: " + dir)
        for (f <- files) { // only consider jars
          if (f.isFile && f.getAbsolutePath.toLowerCase.endsWith(".jar")) {
            val url = f.toURI.toURL
            JarUtils.checkJarFile(url)
            dependencies.add(url)
          }
        }
      }
    } catch {
      case e: Exception =>
        throw new RuntimeException("Could not load all required JAR files.", e)
    }
    if (logger.isDebugEnabled) logger.debug("Using the following dependencies: {}", dependencies)
    dependencies
  }
}
