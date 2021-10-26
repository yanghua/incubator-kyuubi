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

import org.apache.kyuubi.engine.flink.WithFlinkSQLEngine
import org.apache.kyuubi.engine.flink.result.Constants
import org.apache.kyuubi.operation.HiveJDBCTests

class FlinkOperationSuite extends WithFlinkSQLEngine with HiveJDBCTests {
  override def withKyuubiConf: Map[String, String] = Map.empty

  override protected def jdbcUrl: String = getJdbcUrl

  test("get table type for flink sql") {
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      val types = meta.getTableTypes
      val expected = Constants.SUPPORTED_TABLE_TYPES.toIterator
      while (types.next()) {
        assert(types.getString("table_types") === expected.next())
      }
      assert(!expected.hasNext)
      assert(!types.next())
    }
  }

  test("get catalogs for flink sql") {
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      val catalogs = meta.getCatalogs
      val expected = Set("default_catalog").toIterator
      while (catalogs.next()) {
        assert(catalogs.getString("catalogs") === expected.next())
      }
      assert(!expected.hasNext)
      assert(!catalogs.next())
    }
  }

}
