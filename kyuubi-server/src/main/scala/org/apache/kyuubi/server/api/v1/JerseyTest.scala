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

package org.apache.kyuubi.server.api.v1

import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.PathParam
import javax.ws.rs.Produces
import javax.ws.rs.core.MediaType
import org.apache.kyuubi.Utils
import org.eclipse.jetty.server.{HttpConfiguration, HttpConnectionFactory, Server, ServerConnector}
import org.eclipse.jetty.server.handler.{ContextHandlerCollection, ErrorHandler}
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder
import org.eclipse.jetty.util.thread.{QueuedThreadPool, ScheduledExecutorScheduler}
import org.glassfish.jersey.server.ServerProperties
import org.glassfish.jersey.servlet.ServletContainer

object JerseyTest {

  def main(args: Array[String]): Unit = {
    val pool = new QueuedThreadPool
    pool.setName("this.name")
    pool.setDaemon(true)

    val server = new Server(pool)
    val errorHandler = new ErrorHandler()
    errorHandler.setShowStacks(true)
    errorHandler.setServer(server)
    server.addBean(errorHandler)

    val collection = new ContextHandlerCollection
    server.setHandler(collection)

    val serverExecutor = new ScheduledExecutorScheduler(s"JettyScheduler", true)

    var minThreads = 1
    val httpConfig = new HttpConfiguration()

    val connector = new ServerConnector(
      server,
      null,
      serverExecutor,
      null,
      -1,
      -1,
      Array(new HttpConnectionFactory(httpConfig)): _*)
    connector.setPort(10009)
    connector.setHost("localhost")
    connector.setReuseAddress(!Utils.isWindows)

    // Currently we only use "SelectChannelConnector"
    // Limit the max acceptor number to 8 so that we don't waste a lot of threads
    connector.setAcceptQueueSize(math.min(connector.getAcceptors, 8))

    connector.start()
    // The number of selectors always equals to the number of acceptors
    minThreads += connector.getAcceptors * 2
    server.addConnector(connector)
    pool.setMaxThreads(math.max(pool.getMaxThreads, minThreads))

    val servlet = new ServletHolder(classOf[ServletContainer])
    servlet.setInitParameter(
      ServerProperties.PROVIDER_PACKAGES,
      "org.apache.kyuubi.server.api.v1")
    servlet.setInitParameter(
      ServerProperties.PROVIDER_CLASSNAMES,
      "org.glassfish.jersey.jackson.JacksonFeature")
    servlet.setInitParameter("com.sun.jersey.api.json.POJOMappingFeature", "true")
    val handler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
    handler.setContextPath("/api")
    handler.addServlet(servlet, "/*")

    collection.addHandler(handler)

    server.start
    server.join()
  }

}


@Path("hello")
class HelloAction {

  @GET
  def asdf(): String = "hello"

  @GET
  @Path("{name}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  @throws[Exception]
  def hello(@PathParam("name") name: String): String = "{ \"message\": \"asdfas\"}"

  @GET
  @Path("version")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def version(): VersionInfo = {
    val versionInfo = new VersionInfo()
    versionInfo.setNum(1234)
    versionInfo
  }

}
