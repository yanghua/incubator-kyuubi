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

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.kyuubi.engine.flink.config.EnvironmentUtil.readEnvironment;

import java.util.Arrays;
import java.util.Collections;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.kyuubi.engine.flink.context.EngineContext;

/**
 * Utility class for using java utilities in bash scripts.
 */
public class BashJavaUtil {
	private static final String EXECUTION_PREFIX = "BASH_JAVA_UTILS_EXEC_RESULT:";

	public static void main(String[] args) {
		checkArgument(args.length > 0, "Command not specified.");

		switch (Command.valueOf(args[0])) {
			case GET_SERVER_JVM_ARGS:
				getJvmArgs(Arrays.copyOfRange(args, 1, args.length));
				break;
			case GET_EXECUTION_TARGET:
				getExecutionTarget(Arrays.copyOfRange(args, 1, args.length));
				break;
			default:
				// unexpected, Command#valueOf should fail if a unknown command is passed in
				throw new RuntimeException("Unexpected, something is wrong.");
		}
	}

	private static void getJvmArgs(String[] args) {
		EngineOptions options = EngineOptionsParser.parseEngineOptions(args);
		EngineEnvironment defaultEnv = readEnvironment(options.getDefaultConfig().orElse(null));
		String jvmArgs = defaultEnv.getServer().getJvmArgs();
		System.out.println(EXECUTION_PREFIX + jvmArgs);
	}

	@VisibleForTesting
	private static void getExecutionTarget(String[] args) {
		EngineOptions options = EngineOptionsParser.parseEngineOptions(args);
		EngineEnvironment defaultEnv = readEnvironment(options.getDefaultConfig().orElse(null));
		EngineContext context = new EngineContext(defaultEnv, Collections.emptyList());
		String executionTarget = context.getFlinkConfig().getString(DeploymentOptions.TARGET);
		System.out.println(EXECUTION_PREFIX + executionTarget);
	}

	/**
	 * Commands that BashJavaUtil supports.
	 */
	public enum Command {
		/**
		 * Get server jvm args.
		 */
		GET_SERVER_JVM_ARGS,

		/**
		 * Get execution target in Flink config.
		 */
		GET_EXECUTION_TARGET
	}
}
