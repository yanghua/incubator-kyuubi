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
import java.util.{Collections, List, Optional, UUID}
import scala.collection.JavaConversions._
import org.apache.flink.api.dag.Pipeline
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.configuration.{Configuration, DeploymentOptions}
import org.apache.flink.core.execution.JobClient
import org.apache.flink.table.api.{TableColumn, TableEnvironment, TableSchema}
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils
import org.apache.flink.table.types.utils.DataTypeUtils
import org.apache.flink.types.Row
import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.engine.flink.context.{ExecutionContext, SessionContext}
import org.apache.kyuubi.engine.flink.deployment.{ClusterDescriptorAdapterFactory, ProgramDeployer}
import org.apache.kyuubi.engine.flink.result.{BatchResult, ColumnInfo, Result, ResultDescriptor, ResultKind, ResultSet, ResultUtil, TypedResult}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.operation.{OperationState, OperationType}
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.util.ThreadUtils

import java.util.concurrent.{RejectedExecutionException, ScheduledExecutorService, TimeUnit}

class ExecuteStatement(
    sessionContext: SessionContext,
    session: Session,
    protected override val statement: String,
    override val shouldRunAsync: Boolean,
    queryTimeout: Long)
  extends FlinkOperation(sessionContext, OperationType.EXECUTE_STATEMENT, session) with Logging {

  private val operationLog: OperationLog =
    OperationLog.createOperationLog(session.handle, getHandle)

  private var resultDescriptor: ResultDescriptor = _

  private var columnInfos: List[ColumnInfo] = _

  private var statementTimeoutCleaner: Option[ScheduledExecutorService] = None

  override def getOperationLog: Option[OperationLog] = Option(operationLog)

  override protected def beforeRun(): Unit = {
    OperationLog.setCurrentOperationLog(operationLog)
    setState(OperationState.PENDING)
    setHasResultSet(true)
  }

  override protected def afterRun(): Unit = {
    OperationLog.removeCurrentOperationLog()
  }

  override protected def runInternal(): Unit = {
//    executeStatement()

    addTimeoutMonitor()
    if (shouldRunAsync) {
      val asyncOperation = new Runnable {
        override def run(): Unit = {
          OperationLog.setCurrentOperationLog(operationLog)
        }
      }

      try {
        executeStatement()
        val flinkSQLSessionManager = session.sessionManager
        val backgroundHandle = flinkSQLSessionManager.submitBackgroundOperation(asyncOperation)
        setBackgroundHandle(backgroundHandle)
      } catch {
        case rejected: RejectedExecutionException =>
          setState(OperationState.ERROR)
          val ke = KyuubiSQLException("Error submitting query in background, query rejected",
            rejected)
          setOperationException(ke)
          throw ke
      }
    } else {
      executeStatement()
    }
  }

  private def executeStatement(): Unit = {
    try {
      setState(OperationState.RUNNING)
      resultDescriptor = executeQueryInternal(sessionContext.getExecutionContext, statement)
      jobId = resultDescriptor.getJobClient.getJobID

      val resultSchemaColumns: util.List[TableColumn] =
        resultDescriptor.getResultSchema.getTableColumns
      columnInfos = new util.ArrayList[ColumnInfo]
      for (column <- resultSchemaColumns) {
        columnInfos.add(ColumnInfo.create(column.getName, column.getType.getLogicalType))
      }

      val result = resultDescriptor.getResult.asInstanceOf[BatchResult[_]]
      val typedResult = result.retrieveChanges
      var rows: util.List[Row] = null
      if (typedResult.getType eq TypedResult.ResultType.EOS) {
        rows = Collections.emptyList()
      } else if (typedResult.getType eq TypedResult.ResultType.PAYLOAD) {
        rows = typedResult.getPayload
      } else {
        rows = Collections.emptyList()
      }

      resultSet = ResultSet.builder
        .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
        .columns(columnInfos)
        .data(rows)
        .build
      setState(OperationState.FINISHED)
    } catch {
      onError(cancel = true)
    } finally {
      statementTimeoutCleaner.foreach(_.shutdown())
    }
  }

  private def executeQueryInternal[C](
      executionContext: ExecutionContext[C],
      query: String): ResultDescriptor = {
    // create table
    val table = createTable(executionContext, executionContext.getTableEnvironment, query)
    val isChangelogResult = executionContext.getEnvironment.getExecution.inStreamingMode
    // initialize result
    var result: Result[C, _] = null
    if (isChangelogResult) {
      result = ResultUtil.createChangelogResult(
        executionContext.getFlinkConfig,
        executionContext.getEnvironment,
        removeTimeAttributes(table.getSchema),
        executionContext.getExecutionConfig)
    } else {
      result = ResultUtil.createBatchResult(
        removeTimeAttributes(table.getSchema),
        executionContext.getExecutionConfig)
    }
    val jobName = getJobName(query)
    val tableName = String.format("_tmp_table_%s", UUID.randomUUID.toString.replace("-", ""))
    var pipeline: Pipeline = null
    try {
      // writing to a sink requires an optimization step that might reference UDFs
      // during code compilation
      executionContext.wrapClassLoader(() => {
        def foo() = {
          executionContext
            .getTableEnvironment
            .registerTableSinkInternal(tableName, result.getTableSink)
          table.insertInto(tableName)
          null
        }

        foo()
      })
      pipeline = executionContext.createPipeline(jobName)
    } catch {
      case t: Throwable =>
        // the result needs to be closed as long as
        // it not stored in the result store
        result.close()
        logger.error(String.format("Session: %s. Invalid SQL query.", session.handle), t)
        // catch everything such that the query does not crash the executor
        throw new RuntimeException("Invalid SQL query.", t)
    } finally {
      // Remove the temporal table object.
      executionContext.wrapClassLoader(() => {
        def foo() = {
          executionContext.getTableEnvironment.dropTemporaryTable(tableName)
          null
        }

        foo()
      })
    }
    // create a copy so that we can change settings without affecting the original config
    val configuration = new Configuration(executionContext.getFlinkConfig)
    // for queries we wait for the job result, so run in attached mode
    configuration.set[java.lang.Boolean](DeploymentOptions.ATTACHED, true)
    // shut down the cluster if the shell is closed
    configuration.set[java.lang.Boolean](DeploymentOptions.SHUTDOWN_IF_ATTACHED, true)
    // create execution
    val deployer = new ProgramDeployer(configuration, jobName, pipeline, sessionContext.getExecutionContext.getClassLoader)
    setState(OperationState.COMPILED)
    var jobClient: JobClient = null
    // blocking deployment
    try jobClient = deployer.deploy.get
    catch {
      case e: Exception =>
        logger.error(String.format("Session: %s. Error running SQL job.", session.handle), e)
        throw new RuntimeException("Error running SQL job.", e)
    }
    val jobID = jobClient.getJobID
    this.clusterDescriptorAdapter = ClusterDescriptorAdapterFactory.create(
      sessionContext.getExecutionContext, configuration, session.handle.toString, jobID)
    if (logger.isDebugEnabled) {
      logger.debug("Cluster Descriptor Adapter: {}", clusterDescriptorAdapter)
    }
    logger.info("Session: {}. Submit flink job: {} successfully, query: ",
      session.handle.toString, jobID.toString, query)
    // start result retrieval
    result.startRetrieval(jobClient)
    new ResultDescriptor(
      result,
      isChangelogResult,
      removeTimeAttributes(table.getSchema),
      jobClient)
  }

  private def createTable[C](
    context: ExecutionContext[C],
    tableEnv: TableEnvironment,
    selectQuery: String) = {
    // parse and validate query
    try context.wrapClassLoader(() => tableEnv.sqlQuery(selectQuery))
    catch {
      case t: Throwable =>
        // catch everything such that the query does not crash the executor
        throw new RuntimeException("Invalid SQL statement.", t)
    }
  }

  private def removeTimeAttributes(schema: TableSchema) = {
    val builder = TableSchema.builder
    for (i <- 0 until schema.getFieldCount) {
      val dataType = schema.getFieldDataTypes()(i)
      val convertedType = DataTypeUtils.replaceLogicalType(
        dataType, LogicalTypeUtils.removeTimeAttributes(dataType.getLogicalType))
      builder.field(schema.getFieldNames()(i), convertedType)
    }
    builder.build
  }

  private def addTimeoutMonitor(): Unit = {
    if (queryTimeout > 0) {
      val timeoutExecutor =
        ThreadUtils.newDaemonSingleThreadScheduledExecutor("query-timeout-thread")
      timeoutExecutor.schedule(new Runnable {
        override def run(): Unit = {
          cleanup(OperationState.TIMEOUT)
        }
      }, queryTimeout, TimeUnit.SECONDS)
      statementTimeoutCleaner = Some(timeoutExecutor)
    }
  }

  private def fetchBatchResult = {
    val result = resultDescriptor.getResult.asInstanceOf[BatchResult[_]]
    val typedResult = result.retrieveChanges
    if (typedResult.getType eq TypedResult.ResultType.EOS) {
      Optional.empty
    }
    else if (typedResult.getType eq TypedResult.ResultType.PAYLOAD) {
      val payload = typedResult.getPayload
      Optional.of(Tuple2.of(payload, null))
    }
    else Optional.of(Tuple2.of(Collections.emptyList, null))
  }

}
