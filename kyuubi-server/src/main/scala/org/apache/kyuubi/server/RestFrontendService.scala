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

import org.eclipse.jetty.server.{HttpConfiguration, HttpConnectionFactory, Server, ServerConnector}
import org.eclipse.jetty.util.component.LifeCycle
import org.eclipse.jetty.util.thread.{QueuedThreadPool, ScheduledExecutorScheduler}

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_CONNECTION_URL_USE_HOSTNAME, FRONTEND_REST_BIND_HOST, FRONTEND_REST_BIND_PORT}
import org.apache.kyuubi.service.{AbstractFrontendService, BackendService, ServiceState}

class RestFrontendService private(name: String, be: BackendService)
  extends AbstractFrontendService(name, be) with Logging {

  def this(be: BackendService) = {
    this(classOf[RestFrontendService].getSimpleName, be)
  }

  protected var serverAddr: InetAddress = _
  protected var portNum: Int = _
  var server: Server = _


  override def initialize(conf: KyuubiConf): Unit = synchronized {
    val serverHost = conf.get(FRONTEND_REST_BIND_HOST)
    serverAddr = serverHost.map(InetAddress.getByName).getOrElse(Utils.findLocalInetAddress)
    portNum = conf.get(FRONTEND_REST_BIND_PORT)

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


  override def start(): Unit = startJettyServer()

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

  def startJettyServer(): Unit = {
    val pool = new QueuedThreadPool
    pool.setName(this.name)
    pool.setDaemon(true)

    server = new Server(pool)

    val serverExecutor = new ScheduledExecutorScheduler(s"${this.name}-JettyScheduler", true)

    try {
      server.start()

      var minThreads = 1
      val httpConfig = new HttpConfiguration()

      val connector = new ServerConnector(
        server,
        null,
        serverExecutor,
        null,
        -1,
        -1,
        Array(new HttpConnectionFactory(httpConfig)): _*)
      connector.setPort(portNum)
      connector.setHost(serverAddr.getHostName)
      connector.setReuseAddress(!Utils.isWindows)

      // Currently we only use "SelectChannelConnector"
      // Limit the max acceptor number to 8 so that we don't waste a lot of threads
      connector.setAcceptQueueSize(math.min(connector.getAcceptors, 8))

      connector.start()
      // The number of selectors always equals to the number of acceptors
      minThreads += connector.getAcceptors * 2

      server.addConnector(connector)
      pool.setMaxThreads(math.max(pool.getMaxThreads, minThreads))

      super.start()
    } catch {
      case e: Exception =>
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

}
