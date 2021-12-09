/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.engine.flink.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kyuubi.engine.flink.config.entries.CatalogEntry;
import org.apache.kyuubi.engine.flink.config.entries.ConfigurationEntry;
import org.apache.kyuubi.engine.flink.config.entries.DeploymentEntry;
import org.apache.kyuubi.engine.flink.config.entries.EngineEntry;
import org.apache.kyuubi.engine.flink.config.entries.ExecutionEntry;
import org.apache.kyuubi.engine.flink.config.entries.ModuleEntry;
import org.apache.kyuubi.engine.flink.config.entries.SessionEntry;
import org.apache.kyuubi.engine.flink.config.entries.TableEntry;
import org.apache.kyuubi.engine.flink.config.entries.ViewEntry;

/**
 * EngineEnvironment configuration that represents the content of an environment file.
 * EngineEnvironment files define engine, session, catalogs, tables, execution, and deployment
 * behavior. An environment might be defined by default or as part of a session. Environments can be
 * merged or enriched with properties.
 */
public class EngineEnvironment {

  public static final String ENGINE_ENTRY = "engine";

  public static final String SESSION_ENTRY = "session";

  public static final String EXECUTION_ENTRY = "execution";

  public static final String CONFIGURATION_ENTRY = "table";

  public static final String DEPLOYMENT_ENTRY = "deployment";

  private EngineEntry engine;

  private SessionEntry session;

  private Map<String, ModuleEntry> modules;

  private Map<String, CatalogEntry> catalogs;

  private Map<String, TableEntry> tables;

  private ExecutionEntry execution;

  private ConfigurationEntry configuration;

  private DeploymentEntry deployment;

  public EngineEnvironment() {
    this.engine = EngineEntry.DEFAULT_INSTANCE;
    this.session = SessionEntry.DEFAULT_INSTANCE;
    this.modules = new LinkedHashMap<>();
    this.catalogs = Collections.emptyMap();
    this.tables = Collections.emptyMap();
    this.execution = ExecutionEntry.DEFAULT_INSTANCE;
    this.configuration = ConfigurationEntry.DEFAULT_INSTANCE;
    this.deployment = DeploymentEntry.DEFAULT_INSTANCE;
  }

  public void setSession(Map<String, Object> config) {
    this.session = SessionEntry.create(config);
  }

  public SessionEntry getSession() {
    return session;
  }

  public void setEngine(Map<String, Object> config) {
    this.engine = EngineEntry.create(config);
  }

  public EngineEntry getEngine() {
    return engine;
  }

  public Map<String, ModuleEntry> getModules() {
    return modules;
  }

  public Map<String, CatalogEntry> getCatalogs() {
    return catalogs;
  }

  public Map<String, TableEntry> getTables() {
    return tables;
  }

  public ExecutionEntry getExecution() {
    return execution;
  }

  public void setConfiguration(Map<String, Object> config) {
    this.configuration = ConfigurationEntry.create(config);
  }

  public ConfigurationEntry getConfiguration() {
    return configuration;
  }

  public DeploymentEntry getDeployment() {
    return deployment;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("==================== Engine =====================\n");
    engine.asTopLevelMap().forEach((k, v) -> sb.append(k).append(": ").append(v).append('\n'));
    sb.append("==================== Session =====================\n");
    session.asTopLevelMap().forEach((k, v) -> sb.append(k).append(": ").append(v).append('\n'));
    sb.append("===================== Modules =====================\n");
    modules.forEach(
        (name, module) -> {
          sb.append("- ").append(ModuleEntry.MODULE_NAME).append(": ").append(name).append("\n");
          module
              .asMap()
              .forEach((k, v) -> sb.append("  ").append(k).append(": ").append(v).append('\n'));
        });
    sb.append("===================== Catalogs =====================\n");
    catalogs.forEach(
        (name, catalog) -> {
          sb.append("- ").append(CatalogEntry.CATALOG_NAME).append(": ").append(name).append("\n");
          catalog
              .asMap()
              .forEach((k, v) -> sb.append("  ").append(k).append(": ").append(v).append('\n'));
        });
    sb.append("===================== Tables =====================\n");
    tables.forEach(
        (name, table) -> {
          sb.append("- ").append(TableEntry.TABLES_NAME).append(": ").append(name).append("\n");
          table
              .asMap()
              .forEach((k, v) -> sb.append("  ").append(k).append(": ").append(v).append('\n'));
        });
    sb.append("=================== Execution ====================\n");
    execution.asTopLevelMap().forEach((k, v) -> sb.append(k).append(": ").append(v).append('\n'));
    sb.append("================== Configuration =================\n");
    configuration.asMap().forEach((k, v) -> sb.append(k).append(": ").append(v).append('\n'));
    sb.append("=================== Deployment ===================\n");
    deployment.asTopLevelMap().forEach((k, v) -> sb.append(k).append(": ").append(v).append('\n'));
    return sb.toString();
  }

  // --------------------------------------------------------------------------------------------

  /**
   * Merges two environments. The properties of the first environment might be overwritten by the
   * second one.
   */
  public static EngineEnvironment merge(EngineEnvironment env1, EngineEnvironment env2) {
    final EngineEnvironment mergedEnv = new EngineEnvironment();

    // merge engine properties
    mergedEnv.engine = EngineEntry.merge(env1.getEngine(), env2.getEngine());

    // merge session properties
    mergedEnv.session = SessionEntry.merge(env1.getSession(), env2.getSession());

    // merge modules
    final Map<String, ModuleEntry> modules = new LinkedHashMap<>(env1.getModules());
    modules.putAll(env2.getModules());
    mergedEnv.modules = modules;

    // merge catalogs
    final Map<String, CatalogEntry> catalogs = new HashMap<>(env1.getCatalogs());
    catalogs.putAll(env2.getCatalogs());
    mergedEnv.catalogs = catalogs;

    // merge tables
    final Map<String, TableEntry> tables = new LinkedHashMap<>(env1.getTables());
    tables.putAll(env2.getTables());
    mergedEnv.tables = tables;

    // merge execution properties
    mergedEnv.execution = ExecutionEntry.merge(env1.getExecution(), env2.getExecution());

    // merge configuration properties
    mergedEnv.configuration =
        ConfigurationEntry.merge(env1.getConfiguration(), env2.getConfiguration());

    // merge deployment properties
    mergedEnv.deployment = DeploymentEntry.merge(env1.getDeployment(), env2.getDeployment());

    return mergedEnv;
  }

  public EngineEnvironment clone() {
    return enrich(this, Collections.emptyMap(), Collections.emptyMap());
  }

  /** Enriches an environment with new/modified properties or views and returns the new instance. */
  public static EngineEnvironment enrich(
      EngineEnvironment env, Map<String, String> properties, Map<String, ViewEntry> views) {
    final EngineEnvironment enrichedEnv = new EngineEnvironment();

    enrichedEnv.modules = new LinkedHashMap<>(env.getModules());

    // merge catalogs
    enrichedEnv.catalogs = new LinkedHashMap<>(env.getCatalogs());

    // merge tables
    enrichedEnv.tables = new LinkedHashMap<>(env.getTables());
    enrichedEnv.tables.putAll(views);

    // enrich execution properties
    enrichedEnv.execution = ExecutionEntry.enrich(env.execution, properties);

    // enrich configuration properties
    enrichedEnv.configuration = ConfigurationEntry.enrich(env.configuration, properties);

    // enrich deployment properties
    enrichedEnv.deployment = DeploymentEntry.enrich(env.deployment, properties);

    // does not change session properties
    enrichedEnv.session = env.getSession();

    // does not change engine properties
    enrichedEnv.engine = env.getEngine();

    return enrichedEnv;
  }
}
