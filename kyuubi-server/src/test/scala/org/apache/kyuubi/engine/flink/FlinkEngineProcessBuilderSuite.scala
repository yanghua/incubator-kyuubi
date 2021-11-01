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

//import java.io.{BufferedReader, InputStreamReader}
//import java.nio.charset.StandardCharsets

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf

class FlinkEngineProcessBuilderSuite extends KyuubiFunSuite {
  private def conf = KyuubiConf().set("kyuubi.on", "off")

  test("flink engine process builder") {
    val builder = new FlinkEngineProcessBuilder("vinoyang", conf)
    val commands = builder.toString.split(' ')
    val pb = new ProcessBuilder(commands.head)
    val process = builder.start
//    pb.redirectErrorStream(true)
//    pb.environment().put("FLINK_HOME", builder.getFlinkHome)
//    pb.environment().put("FLINK_CONF_DIR", builder.getFlinkHome + "/conf")

//    val stdout = pb.start()

//    try {
//      val bufferedReader: BufferedReader = new BufferedReader(
//        new InputStreamReader(stdout.getInputStream, StandardCharsets.UTF_8))
//      while (stdout.isAlive) {
//        while (bufferedReader.ready()) {
//          val s = bufferedReader.readLine()
//          // scalastyle:off println
//          println(s)
//          if (s === "started engine...") {
////            stdout.destroyForcibly()
//          }
//        }
//      }
//    } finally {
//      stdout.destroyForcibly()
//    }
  }

}
