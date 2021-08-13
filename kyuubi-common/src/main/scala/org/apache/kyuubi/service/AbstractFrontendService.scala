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

package org.apache.kyuubi.service

import java.net.InetAddress

import org.apache.kyuubi.{KyuubiException, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_CONNECTION_URL_USE_HOSTNAME, FRONTEND_BIND_HOST, FRONTEND_BIND_PORT, FRONTEND_LOGIN_TIMEOUT}

abstract class AbstractFrontendService(name: String, be: BackendService)
  extends CompositeService(name) {

  protected var serverAddr: InetAddress = _
  protected var portNum: Int = _
  @volatile protected var isStarted = false
  protected var requestTimeout: Int = _

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    this.conf = conf
    try {
      val serverHost = conf.get(FRONTEND_BIND_HOST)
      serverAddr = serverHost.map(InetAddress.getByName).getOrElse(Utils.findLocalInetAddress)
      portNum = conf.get(FRONTEND_BIND_PORT)
      requestTimeout = conf.get(FRONTEND_LOGIN_TIMEOUT).toInt
    } catch {
      case e: Throwable =>
        throw new KyuubiException(
          s"Failed to initialize frontend service on $serverAddr:$portNum.", e)
    }


    super.initialize(conf)
  }

  def connectionUrl(server: Boolean): String = {
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


}
