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

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_CONNECTION_URL_USE_HOSTNAME}
import org.apache.kyuubi.ha.client.{EngineServiceDiscovery, ServiceDiscovery}
import org.apache.kyuubi.service.{Serverable, Service, ThriftBinaryFrontendService}

class FlinkThriftBinaryFrontendService(
    override val serverable: Serverable)
  extends ThriftBinaryFrontendService("FlinkThriftBinaryFrontendService", serverable) {

  override def connectionUrl: String = {
    checkInitialized()
    if (conf.get(ENGINE_CONNECTION_URL_USE_HOSTNAME)) {
      s"${serverAddr.getCanonicalHostName}:$portNum"
    } else {
      // engine use address if run on k8s with cluster mode
      s"${serverAddr.getHostAddress}:$portNum"
    }
  }

  override def initialize(conf: KyuubiConf): Unit = {
//    conf.set(FRONTEND_THRIFT_BINARY_BIND_PORT, 10019)
//    conf.set(HA_ZK_QUORUM, "192.168.13.110:2181")
    super.initialize(conf)
  }


  override def start(): Unit = {
    super.start()
  }

  override lazy val discoveryService: Option[Service] = {
    if (ServiceDiscovery.supportServiceDiscovery(conf)) {
      Some(new EngineServiceDiscovery(this))
    } else {
//      val zkServer = new EmbeddedZookeeper()
//      zkServer.initialize(conf)
//      zkServer.start()
//      conf.set(HA_ZK_QUORUM, zkServer.getConnectString)
//      conf.set(HA_ZK_ACL_ENABLED, false)
      Some(new EngineServiceDiscovery(this))
      None
    }
  }
}
