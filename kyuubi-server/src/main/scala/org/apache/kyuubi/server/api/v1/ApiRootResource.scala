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

import javax.ws.rs.{GET, Path, Produces}
import javax.ws.rs.core.MediaType

import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.glassfish.jersey.server.ServerProperties
import org.glassfish.jersey.servlet.ServletContainer

import org.apache.kyuubi.service.BackendService

@Path("/v1")
private[v1] class ApiRootResource extends ApiRequestContext {

  @Path("sessions/{identifier}")
  def session(): Class[OneSessionResource] = classOf[OneSessionResource]

  @GET
  @Path("version")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def version(): VersionInfo = {
    val versionInfo = new VersionInfo()
    versionInfo.setNum(1234)
    versionInfo
  }

  @GET
  @Path("ping")
  @Produces(Array(MediaType.TEXT_PLAIN))
  def ping(): String = "pong"

}

private[server] object ApiRootResource {

  def getServletHandler(backendService: BackendService): ServletContextHandler = {
    val servlet = new ServletHolder(classOf[ServletContainer])
    servlet.setInitParameter(
      ServerProperties.PROVIDER_PACKAGES,
      "org.apache.kyuubi.server.api.v1")
    servlet.setInitParameter(
      ServerProperties.PROVIDER_CLASSNAMES,
      "org.glassfish.jersey.jackson.JacksonFeature")
    val handler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
    BackendServiceProvider.setBackendService(handler, backendService)
    handler.setContextPath("/api")
    handler.addServlet(servlet, "/*")
    handler
  }

}
