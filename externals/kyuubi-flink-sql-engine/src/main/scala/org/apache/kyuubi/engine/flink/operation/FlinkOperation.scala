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

import org.apache.commons.lang3.StringUtils
import org.apache.flink.table.types.logical.{BigIntType, BooleanType, DecimalType, DoubleType, FloatType, IntType, LogicalType, NullType, TinyIntType, VarCharType}
import org.apache.flink.types.Row
import org.apache.hive.service.rpc.thrift.{TBoolColumn, TBoolValue, TCLIServiceConstants, TColumn, TColumnDesc, TColumnValue, TDoubleValue, TI16Column, TI16Value, TI32Value, TI64Value, TPrimitiveTypeEntry, TProtocolVersion, TRow, TRowSet, TStringColumn, TStringValue, TTableSchema, TTypeDesc, TTypeEntry, TTypeId, TTypeQualifierValue, TTypeQualifiers}
import org.apache.kyuubi.engine.flink.context.SessionContext
import org.apache.kyuubi.engine.flink.{FetchIterator, IterableFetchIterator}
import org.apache.kyuubi.engine.flink.result.{ColumnInfo, ResultSet}
import org.apache.kyuubi.operation.{AbstractOperation, OperationState}
import org.apache.kyuubi.operation.FetchOrientation.{FETCH_FIRST, FETCH_NEXT, FETCH_PRIOR, FetchOrientation}
import org.apache.kyuubi.operation.OperationState.OperationState
import org.apache.kyuubi.operation.OperationType.OperationType
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session

import java.io.IOException
import java.nio.ByteBuffer
import java.time.ZoneId
import java.util
import java.util.Collections
import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, mapAsJavaMapConverter, seqAsJavaListConverter}
import scala.language.implicitConversions

abstract class FlinkOperation(
    sessionContext: SessionContext, opType: OperationType, session: Session)
  extends AbstractOperation(opType, session) {

  private val timeZone: ZoneId = {
    // TODO : fetch it from flink
    ZoneId.systemDefault()
  }

  protected var iter: FetchIterator[Row] = null

  protected var resultSet: ResultSet = _

  override protected def beforeRun(): Unit = {
    setHasResultSet(true)
    setState(OperationState.RUNNING)
  }

  override protected def afterRun(): Unit = {
    state.synchronized {
      if (!isTerminalState(state)) {
        setState(OperationState.FINISHED)
      }
    }
    OperationLog.removeCurrentOperationLog()
  }

  override def cancel(): Unit = {
    cleanup(OperationState.CANCELED)
  }

  override def close(): Unit = {
    cleanup(OperationState.CLOSED)
    try {
      getOperationLog.foreach(_.close())
    } catch {
      case e: IOException =>
        error(e.getMessage, e)
    }
  }

  override def getResultSetSchema: TTableSchema = {
    val tTableSchema = new TTableSchema()
    resultSet.getColumns.asScala.zipWithIndex.foreach { case (f, i) =>
      tTableSchema.addToColumns(FlinkOperation.toTColumnDesc(f, i))
    }
    tTableSchema
  }

  override def getNextRowSet(order: FetchOrientation, rowSetSize: Int): TRowSet = {
    logger.info("Invoke getNextRowSet")
    validateDefaultFetchOrientation(order)
    assertState(OperationState.FINISHED)
    setHasResultSet(true)
    if (iter == null) {
      iter = new IterableFetchIterator(resultSet.getData.asScala)
    }
    order match {
      case FETCH_NEXT => iter.fetchNext()
      case FETCH_PRIOR => iter.fetchPrior(rowSetSize);
      case FETCH_FIRST => iter.fetchAbsolute(0);
    }
    val token = iter.take(rowSetSize)
    val resultRowSet = FlinkOperation.resultSetToTRowSet(
      token.toList, resultSet, getProtocolVersion, timeZone)
    resultRowSet.setStartRowOffset(iter.getPosition)
    resultRowSet
  }

  override def shouldRunAsync: Boolean = false

  protected def cleanup(targetState: OperationState): Unit = state.synchronized {
    if (!isTerminalState(state)) {
      setState(targetState)
      Option(getBackgroundHandle).foreach(_.cancel(true))
    }
  }

}

object FlinkOperation {

  def resultSetToTRowSet(
      rows: Seq[Row],
      resultSet: ResultSet,
      protocolVersion: TProtocolVersion,
      timeZone: ZoneId): TRowSet = {
    if (protocolVersion.getValue < TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6.getValue) {
      toRowBaseSet(rows, resultSet, timeZone)
    } else {
      toColumnBasedSet(rows, resultSet, timeZone)
    }
  }

  def toRowBaseSet(rows: Seq[Row], resultSet: ResultSet, timeZone: ZoneId): TRowSet = {
    val tRows = rows.map { row =>
      val tRow = new TRow()
      (0 until row.getArity).map(i => toTColumnValue(i, row, resultSet, timeZone))
        .foreach(tRow.addToColVals)
      tRow
    }.asJava

    new TRowSet(0, tRows)
  }

  def toColumnBasedSet(rows: Seq[Row], resultSet: ResultSet, timeZone: ZoneId): TRowSet = {
    val size = rows.length
    val tRowSet = new TRowSet(0, new util.ArrayList[TRow](size))
    resultSet.getColumns.asScala.zipWithIndex.foreach { case (filed, i) =>
      val tColumn = toTColumn(rows, i, filed.getLogicalType, timeZone)
      tRowSet.addToColumns(tColumn)
    }
    tRowSet
  }

  private def toTColumnValue(
      ordinal: Int,
      row: Row,
      resultSet: ResultSet,
      timeZone: ZoneId): TColumnValue = {

    val logicalType = resultSet.getColumns.get(ordinal).getLogicalType

    if (logicalType.isInstanceOf[BooleanType]) {
      val boolValue = new TBoolValue
      if (row.getField(ordinal) != null) {
        boolValue.setValue(row.getField(ordinal).asInstanceOf[Boolean])
      }
      TColumnValue.boolVal(boolValue)
    } else if (logicalType.isInstanceOf[TinyIntType]) {
      val tI16Value = new TI16Value
      if (row.getField(ordinal) != null) {
        tI16Value.setValue(row.getField(ordinal).asInstanceOf[Short])
      }
      TColumnValue.i16Val(tI16Value)
    } else if (logicalType.isInstanceOf[IntType]) {
      val tI32Value = new TI32Value
      if (row.getField(ordinal) != null) {
        tI32Value.setValue(row.getField(ordinal).asInstanceOf[Short])
      }
      TColumnValue.i32Val(tI32Value)
    } else if (logicalType.isInstanceOf[BigIntType]) {
      val tI64Value = new TI64Value
      if (row.getField(ordinal) != null) {
        tI64Value.setValue(row.getField(ordinal).asInstanceOf[Long])
      }
      TColumnValue.i64Val(tI64Value)
    } else if (logicalType.isInstanceOf[FloatType]) {
      val tDoubleValue = new TDoubleValue
      if (row.getField(ordinal) != null) {
        tDoubleValue.setValue(row.getField(ordinal).asInstanceOf[Float])
      }
      TColumnValue.doubleVal(tDoubleValue)
    } else if (logicalType.isInstanceOf[DoubleType]) {
      val tDoubleValue = new TDoubleValue
      if (row.getField(ordinal) != null) {
        tDoubleValue.setValue(row.getField(ordinal).asInstanceOf[Double])
      }
      TColumnValue.doubleVal(tDoubleValue)
    } else if (logicalType.isInstanceOf[VarCharType]) {
      val tStringValue = new TStringValue
      if (row.getField(ordinal) != null) {
        tStringValue.setValue(row.getField(ordinal).asInstanceOf[String])
      }
      TColumnValue.stringVal(tStringValue)
    } else {
      val tStrValue = new TStringValue
      if (row.getField(ordinal) != null) {
        // TODO to be down
      }
      TColumnValue.stringVal(tStrValue)
    }
  }

  implicit private def bitSetToBuffer(bitSet: java.util.BitSet): ByteBuffer = {
    ByteBuffer.wrap(bitSet.toByteArray)
  }

  private def toTColumn(
      rows: Seq[Row],
      ordinal: Int,
      logicalType: LogicalType,
      timeZone: ZoneId): TColumn = {
    val nulls = new java.util.BitSet()
    if (logicalType.isInstanceOf[BooleanType]) {
      val values = getOrSetAsNull[java.lang.Boolean](
        rows, ordinal, nulls, true)
      TColumn.boolVal(new TBoolColumn(values, nulls))
    } else if (logicalType.isInstanceOf[TinyIntType]) {
      val values = getOrSetAsNull[java.lang.Short](
        rows, ordinal, nulls, 0.toShort)
      TColumn.i16Val(new TI16Column(values, nulls))
    } else if (logicalType.isInstanceOf[VarCharType]) {
      val values = getOrSetAsNull[java.lang.String](
        rows, ordinal, nulls, "")
      TColumn.stringVal(new TStringColumn(values, nulls))
    } else {
      null
    }
  }

  private def getOrSetAsNull[T](
      rows: Seq[Row],
      ordinal: Int,
      nulls: java.util.BitSet,
      defaultVal: T): java.util.List[T] = {
    val size = rows.length
    val ret = new java.util.ArrayList[T](size)
    var idx = 0
    while (idx < size) {
      val row = rows(idx)
      val isNull = row.getField(ordinal) == null
      if (isNull) {
        nulls.set(idx, true)
        ret.add(idx, defaultVal)
      } else {
        ret.add(idx, row.getField(ordinal).asInstanceOf[T])
      }
      idx += 1
    }
    ret
  }

  def toTColumnDesc(field: ColumnInfo, pos: Int): TColumnDesc = {
    val tColumnDesc = new TColumnDesc()
    tColumnDesc.setColumnName(field.getName)
    tColumnDesc.setTypeDesc(toTTypeDesc(field.getLogicalType))
    tColumnDesc.setComment("")
    tColumnDesc.setPosition(pos)
    tColumnDesc
  }

  def toTTypeDesc(typ: LogicalType): TTypeDesc = {
    val typeEntry = new TPrimitiveTypeEntry(toTTypeId(typ))
    typeEntry.setTypeQualifiers(toTTypeQualifiers(typ))
    val tTypeDesc = new TTypeDesc()
    tTypeDesc.addToTypes(TTypeEntry.primitiveEntry(typeEntry))
    tTypeDesc
  }

  def toTTypeQualifiers(typ: LogicalType): TTypeQualifiers = {
    val ret = new TTypeQualifiers()
    val qualifiers = typ match {
      case d: DecimalType =>
        Map(TCLIServiceConstants.PRECISION -> TTypeQualifierValue.i32Value(d.getPrecision),
          TCLIServiceConstants.SCALE -> TTypeQualifierValue.i32Value(d.getScale)).asJava
      case _ => Collections.emptyMap[String, TTypeQualifierValue]()
    }
    ret.setQualifiers(qualifiers)
    ret
  }

  def toTTypeId(typ: LogicalType): TTypeId = if (typ.isInstanceOf[NullType]) {
    TTypeId.NULL_TYPE
  } else if (typ.isInstanceOf[BooleanType]) {
    TTypeId.BOOLEAN_TYPE
  } else if (typ.isInstanceOf[FloatType]) {
    TTypeId.FLOAT_TYPE
  } else if (typ.isInstanceOf[DoubleType]) {
    TTypeId.DOUBLE_TYPE
  } else if (typ.isInstanceOf[VarCharType]) {
    TTypeId.STRING_TYPE
  } else {
    null
  }

  def toJavaRegex(input: String): String = {
    val res = if (StringUtils.isEmpty(input) || input == "*") {
      "%"
    } else {
      input
    }
    val wStr = ".*"
    res
      .replaceAll("([^\\\\])%", "$1" + wStr).replaceAll("\\\\%", "%").replaceAll("^%", wStr)
      .replaceAll("([^\\\\])_", "$1.").replaceAll("\\\\_", "_").replaceAll("^_", ".")
  }

}
