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

import java.io.{File, FilenameFilter}
import java.net.URI
import java.nio.file.{Files, Path, Paths}

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.{KYUUBI_VERSION, Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_FLINK_MAIN_RESOURCE
import org.apache.kyuubi.engine.ProcBuilder
import org.apache.kyuubi.engine.flink.FlinkEngineContainerBuilder.PROXY_USER

/**
 * A builder to build flink sql engine progress.
 */
class FlinkEngineContainerBuilder(
    override val proxyUser: String,
    override val conf: KyuubiConf)
  extends ProcBuilder with Logging{

  override protected def executable: String = {
    val flinkHomeOpt = env.get("JAVA_HOME").orElse {
      val cwd = getClass.getProtectionDomain.getCodeSource.getLocation.getPath
        .split("kyuubi-server")
      assert(cwd.length > 1)
      Option(
        Paths.get(cwd.head)
          .resolve("externals")
          .resolve("kyuubi-download")
          .resolve("target")
          .toFile
          .listFiles(new FilenameFilter {
            override def accept(dir: File, name: String): Boolean = {
              dir.isDirectory && name.startsWith("flink-")}}))
        .flatMap(_.headOption)
        .map(_.getAbsolutePath)
    }

    env.get("JAVA_HOME").get + "/bin/java"
  }

  override protected def mainResource: Option[String] = {
    val jarName = s"${module}-$KYUUBI_VERSION.jar"
    conf.get(ENGINE_FLINK_MAIN_RESOURCE).filter { userSpecified =>
      // skip check exist if not local file.
      val uri = new URI(userSpecified)
      val schema = if (uri.getScheme != null) uri.getScheme else "file"
      schema match {
        case "file" => Files.exists(Paths.get(userSpecified))
        case _ => true
      }
    }.orElse {
      // 2. get the main resource jar from system build default
      env.get(KyuubiConf.KYUUBI_HOME)
        .map { Paths.get(_, "externals", "engines", "flink", jarName) }
        .filter(Files.exists(_)).map(_.toAbsolutePath.toFile.getCanonicalPath)
    }.orElse {
      // 3. get the main resource from dev environment
      Option(Paths.get("externals", module, "target", jarName))
        .filter(Files.exists(_)).orElse {
        Some(Paths.get("..", "externals", module, "target", jarName))
      }.map(_.toAbsolutePath.toFile.getCanonicalPath)
    }
  }

  override protected def module: String = "kyuubi-flink-sql-engine"

  override protected def mainClass: String = "org.apache.kyuubi.engine.flink.FlinkSQLEngine"

  override protected def commands: Array[String] = {
    val buffer = new ArrayBuffer[String]()
    buffer += "nohup"
    buffer += executable
    conf.getAll.foreach { case (k, v) =>
      buffer += s"-D$k=$v"
    }
    buffer += "-cp"
    buffer += "/Users/vinoyang/Documents/projects/apache-kyuubi-1.4.0-SNAPSHOT-bin-test/jars/*:" +
      "/Users/vinoyang/Documents/projects/apache-kyuubi-1.4.0-SNAPSHOT-bin-test/" +
      "externals/engines/flink/kyuubi-flink-sql-engine-1.4.0-SNAPSHOT.jar"
    buffer += mainClass

    // iff the keytab is specified, PROXY_USER is not supported
    if (!useKeytab()) {
      buffer += PROXY_USER
      buffer += proxyUser
    }

    mainResource.foreach { r => buffer += r }
    buffer += "&"
    buffer.toArray
  }

  override protected val workingDir: Path = {
    env.get("KYUUBI_WORK_DIR_ROOT").map { root =>
      val workingRoot = Paths.get(root).toAbsolutePath
      if (!Files.exists(workingRoot)) {
        debug(s"Creating KYUUBI_WORK_DIR_ROOT at $workingRoot")
        Files.createDirectories(workingRoot)
      }
      if (Files.isDirectory(workingRoot)) {
        workingRoot.toString
      } else null
    }.map { rootAbs =>
      val working = Paths.get(rootAbs, proxyUser)
      if (!Files.exists(working)) {
        debug(s"Creating $proxyUser's working directory at $working")
        Files.createDirectories(working)
      }
      if (Files.isDirectory(working)) {
        working
      } else {
        Utils.createTempDir(rootAbs, proxyUser)
      }
    }.getOrElse {
      Utils.createTempDir(namePrefix = proxyUser)
    }
  }

  private def useKeytab(): Boolean = {
    // TODO : TBD
    false
  }
}

object FlinkEngineContainerBuilder {
  final val APP_KEY = "spark.app.name"
  final val TAG_KEY = "spark.yarn.tags"

  private final val CONF = "--conf"
  private final val CLASS = "--class"
  private final val PROXY_USER = "--proxy-user"
  private final val PRINCIPAL = "spark.kerberos.principal"
  private final val KEYTAB = "spark.kerberos.keytab"
  // Get the appropriate spark-submit file
  private final val FLINK_SUBMIT_FILE = if (Utils.isWindows) "spark-submit.cmd" else "spark-submit"
}
