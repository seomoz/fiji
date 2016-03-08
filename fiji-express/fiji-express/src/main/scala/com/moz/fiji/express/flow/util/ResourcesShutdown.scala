/**
 * (c) Copyright 2014 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.moz.fiji.express.flow.util

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.schema.util.CloseClusterConnectionsShutdownHook

/**
 * Registers a shutdown hook for resources used by FijiExpress.  Must be referenced in each
 * process so that this class is loaded into the JVM and registers its hook.
 */
@ApiAudience.Private
@ApiStability.Experimental
private[express] object ResourcesShutdown {
  def initialize(): Unit = {
    // No-op.  Must be called somewhere to make sure this gets loaded.
  }
  Runtime.getRuntime.addShutdownHook(new CloseClusterConnectionsShutdownHook())
}
