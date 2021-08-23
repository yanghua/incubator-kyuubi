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

import java.util.Locale

import org.scalatest.time.SpanSugar._
import scala.io.Source

import org.apache.kyuubi.{KyuubiException, KyuubiFunSuite}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.{NoopBackendService, Serverable, Service}
import org.apache.kyuubi.service.ServiceState._

class RestFrontendServiceSuite extends KyuubiFunSuite{

  test("kyuubi rest frontend service basic") {
    val server = new RestNoopServer()
    server.stop()
    val conf = KyuubiConf()
    assert(server.getServices.isEmpty)
    assert(server.getServiceState === LATENT)
    val e = intercept[IllegalStateException](server.connectionUrl)
    assert(e.getMessage === "Illegal Service State: LATENT")
    assert(server.getConf === null)

    server.initialize(conf)
    assert(server.getServiceState === INITIALIZED)
    val frontendService = server.getServices(1).asInstanceOf[RestFrontendService]
    assert(frontendService.getServiceState == INITIALIZED)
    assert(server.connectionUrl.split(":").length === 2)
    assert(server.getConf === conf)
    assert(server.getStartTime === 0)
    server.stop()

    server.start()
    assert(server.getServiceState === STARTED)
    assert(frontendService.getServiceState == STARTED)
    assert(server.getStartTime !== 0)
    logger.info(frontendService.connectionUrl(false))

    server.stop()
    assert(server.getServiceState === STOPPED)
    assert(frontendService.getServiceState == STOPPED)
    server.stop()
  }

  test("kyuubi rest frontend service http basic") {
    val server = new RestNoopServer()
    server.stop()
    val conf = KyuubiConf()


    server.initialize(conf)
    assert(server.getServiceState === INITIALIZED)
    val frontendService = server.getServices(1).asInstanceOf[RestFrontendService]
    assert(frontendService.getServiceState == INITIALIZED)
    assert(server.connectionUrl.split(":").length === 2)
    assert(server.getConf === conf)
    assert(server.getStartTime === 0)
    server.stop()

    server.start()
    assert(server.getServiceState === STARTED)
    assert(frontendService.getServiceState == STARTED)
    assert(server.getStartTime !== 0)
    logger.info(frontendService.connectionUrl(false))

    eventually(timeout(10.seconds), interval(50.milliseconds)) {
      val html = Source.fromURL("http://localhost:10009/api/v1/ping").mkString
      assert(html.toLowerCase(Locale.ROOT).equals("pong"))
    }

    server.stop()
  }

  test("kyuubi rest frontend service for one session") {
    val server = new RestNoopServer()
    server.stop()
    val conf = KyuubiConf()


    server.initialize(conf)
    assert(server.getServiceState === INITIALIZED)
    val frontendService = server.getServices(1).asInstanceOf[RestFrontendService]
    assert(frontendService.getServiceState == INITIALIZED)
    assert(server.connectionUrl.split(":").length === 2)
    assert(server.getConf === conf)
    assert(server.getStartTime === 0)

    server.start()
    assert(server.getServiceState === STARTED)
    assert(frontendService.getServiceState == STARTED)

    eventually(timeout(10.seconds), interval(50.milliseconds)) {
      val html = Source.fromURL("http://localhost:10009/api/v1/sessions/asdf").mkString
      assert(html.toLowerCase(Locale.ROOT).equals("pong"))
    }

    server.stop()
  }

  class RestNoopServer extends Serverable("noop") {

    override val backendService = new NoopBackendService
    override val frontendService = new RestFrontendService(backendService)

    override def start(): Unit = {
      super.start()
      if (getConf.getOption("kyuubi.test.server.should.fail").exists(_.toBoolean)) {
        throw new IllegalArgumentException("should fail")
      }
    }

    override protected def stopServer(): Unit = {
      throw new KyuubiException("no need to stop me")
    }

    override val discoveryService: Service = backendService
    override protected val supportsServiceDiscovery: Boolean = false
  }

}
