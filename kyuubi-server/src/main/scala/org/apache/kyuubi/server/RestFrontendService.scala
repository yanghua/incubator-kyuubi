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

package org.apache.kyuubi.server

import java.net.InetAddress

import scala.collection.mutable.ArrayBuffer
import org.eclipse.jetty.server.{HttpConfiguration, HttpConnectionFactory, Server, ServerConnector}
import org.eclipse.jetty.util.component.LifeCycle
import org.eclipse.jetty.util.thread.{QueuedThreadPool, ScheduledExecutorScheduler}
import org.eclipse.jetty.server.handler.{ContextHandlerCollection, ErrorHandler}
import org.eclipse.jetty.servlet.{ServletContextHandler}

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_CONNECTION_URL_USE_HOSTNAME, FRONTEND_REST_BIND_HOST, FRONTEND_REST_BIND_PORT}
import org.apache.kyuubi.server.api.v1.ApiRootResource
import org.apache.kyuubi.service.{AbstractFrontendService, BackendService, ServiceState}

private[server] class RestFrontendService private(name: String, be: BackendService)
  extends AbstractFrontendService(name, be) with Logging {

  def this(be: BackendService) = {
    this(classOf[RestFrontendService].getSimpleName, be)
  }

  protected var serverAddr: InetAddress = _
  protected var portNum: Int = _
  var pool: QueuedThreadPool = _
  var server: Server = _
  var serverExecutor: ScheduledExecutorScheduler = _
  var connector: ServerConnector = _
  protected val handlers = ArrayBuffer[ServletContextHandler]()


  override def initialize(conf: KyuubiConf): Unit = synchronized {
    val serverHost = conf.get(FRONTEND_REST_BIND_HOST)
    serverAddr = serverHost.map(InetAddress.getByName).getOrElse(Utils.findLocalInetAddress)
    portNum = conf.get(FRONTEND_REST_BIND_PORT)

    // init thread pool
    pool = new QueuedThreadPool
    pool.setName(this.name)
    pool.setDaemon(true)

    server = new Server(pool)

    // set error handler
    val errorHandler = new ErrorHandler()
    errorHandler.setShowStacks(true)
    errorHandler.setServer(server)
    server.addBean(errorHandler)

    val handlerCollection = new ContextHandlerCollection
    server.setHandler(handlerCollection)

    serverExecutor = new ScheduledExecutorScheduler(s"${this.name}-JettyScheduler", true)

    connector = new ServerConnector(
      server,
      null,
      serverExecutor,
      null,
      -1,
      -1,
      Array(new HttpConnectionFactory(new HttpConfiguration())): _*)
    connector.setPort(portNum)
    connector.setHost("localhost")
    connector.setReuseAddress(!Utils.isWindows)
    connector.setAcceptQueueSize(math.min(connector.getAcceptors, 8))

    handlerCollection.addHandler(ApiRootResource.getServletHandler(be))

    super.initialize(conf)
  }

  override def connectionUrl(server: Boolean = false): String = {
    getServiceState match {
      case s @ ServiceState.LATENT => throw new IllegalStateException(s"Illegal Service State: $s")
      case _ =>
        if (server || conf.get(ENGINE_CONNECTION_URL_USE_HOSTNAME)) {
          s"${serverAddr.getCanonicalHostName}:$portNum"
        } else {
          // engine use address if run on k8s with cluster mode
          s"${serverAddr.getHostAddress}:$portNum"
        }
    }
  }

  override def start(): Unit = {
    try {
      connector.start()

      // The number of selectors always equals to the number of acceptors
      var minThreads = 1
      minThreads += connector.getAcceptors * 2
      server.addConnector(connector)
      pool.setMaxThreads(math.max(pool.getMaxThreads, minThreads))

      server.start()

      super.start()
    } catch {
      case e: Exception =>
        connector.stop()

        server.stop()
        if (serverExecutor.isStarted) {
          serverExecutor.stop()
        }

        if (pool.isStarted) {
          pool.stop()
        }
        throw e
    }
  }

  override def stop(): Unit = {
    try {
      if (server != null && server.isStarted) {
        server.stop()

        val threadPool = server.getThreadPool
        if (threadPool != null && threadPool.isInstanceOf[LifeCycle]) {
          threadPool.asInstanceOf[LifeCycle].stop
        }
      }
    } catch {
      case e: Exception => throw e
    } finally {
      super.stop()
    }
  }

//  def getServletHandler(backendService: BackendService): ServletContextHandler = {
//    val servlet = new ServletHolder(classOf[ServletContainer])
//    servlet.setInitParameter(
//      ServerProperties.PROVIDER_PACKAGES,
//      "org.apache.kyuubi.server.api.v1")
//    val handler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
//    UIRootFromServletContext
//    handler.setContextPath("/api")
//    handler.addServlet(servlet, "/*")
//    handler
//  }

}
