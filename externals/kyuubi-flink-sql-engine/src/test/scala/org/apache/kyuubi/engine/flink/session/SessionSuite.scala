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

import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.config.KyuubiConf.ENGINE_SHARE_LEVEL
import org.apache.kyuubi.engine.flink.WithFlinkSQLEngine
import org.apache.kyuubi.operation.JDBCTestUtils
import org.apache.kyuubi.service.ServiceState._

class SessionSuite extends WithFlinkSQLEngine with JDBCTestUtils {
  override def withKyuubiConf: Map[String, String] = {
    Map(ENGINE_SHARE_LEVEL.key -> "CONNECTION")
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    startFlinkEngine()
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    stopFlinkEngine()
  }

  override protected def jdbcUrl: String =
    s"jdbc:hive2://${engine.frontendServices.head.connectionUrl}/"

  test("release session if shared level is CONNECTION") {
    logger.info(s"jdbc url is $jdbcUrl")
    assert(engine.getServiceState == STARTED)
    withJdbcStatement() {_ => }
    eventually(Timeout(20.seconds)) {
      assert(engine.getServiceState == STOPPED)
    }
  }
}
