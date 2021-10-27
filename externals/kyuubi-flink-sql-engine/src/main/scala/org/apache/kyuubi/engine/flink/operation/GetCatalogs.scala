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

package org.apache.kyuubi.engine.flink.operation

import java.util
import scala.collection.JavaConverters._

import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.bridge.java.internal.BatchTableEnvironmentImpl
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_TYPE
import org.apache.flink.table.catalog.{CatalogManager, GenericInMemoryCatalog}
import org.apache.flink.table.module.ModuleManager

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.engine.flink.config.EngineEnvironment
import org.apache.kyuubi.engine.flink.config.entries.CatalogEntry.CATALOG_NAME
import org.apache.kyuubi.engine.flink.context.SessionContext
import org.apache.kyuubi.engine.flink.result.{Constants, OperationUtil}
import org.apache.kyuubi.engine.flink.session.FlinkSessionImpl
import org.apache.kyuubi.engine.flink.shim.FlinkCatalogShim
import org.apache.kyuubi.operation.OperationType
import org.apache.kyuubi.session.Session

class GetCatalogs(sessionContext: SessionContext, session: Session)
  extends FlinkOperation(sessionContext, OperationType.GET_CATALOGS, session) {

  override protected def runInternal(): Unit = {
    logger.info("Invoke runInternal...")

    try {
      if (session.isInstanceOf[FlinkSessionImpl]) {
        val tableEnv = sessionContext.getExecutionContext.getTableEnvironment
        val catalogs: java.util.List[String] =
          FlinkCatalogShim().getCatalogs(tableEnv).toList.asJava
        resultSet = OperationUtil.stringListToResultSet(
          catalogs, Constants.SHOW_CATALOGS_RESULT)
      }
    } catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        throw KyuubiSQLException(e)
    }
  }

}

object GetCatalogs{

  def tempCreateTableEnv(env: ExecutionEnvironment): TableEnvironmentInternal = {
    val config = new TableConfig
    val environment = new EngineEnvironment()

    val settings = environment.getExecution.getEnvironmentSettings

    new BatchTableEnvironmentImpl(
      env,
      new TableConfig,
      CatalogManager.newBuilder()
        .classLoader(Thread.currentThread().getContextClassLoader)
        .config(config.getConfiguration)
        .defaultCatalog(settings.getBuiltInCatalogName,
          new GenericInMemoryCatalog(
            settings.getBuiltInCatalogName, settings.getBuiltInDatabaseName))
        .build(),
      new ModuleManager)
  }

  private def createCatalog(name: String, `type`: String) = {
    val prop = new util.HashMap[String, AnyRef]
    prop.put(CATALOG_NAME, name)
    prop.put(CATALOG_TYPE, `type`)
    prop
  }

}
