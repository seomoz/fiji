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

import com.moz.fiji.schema.layout.FijiTableLayout
import com.moz.fiji.schema.layout.FijiTableLayouts

/**
 * Testing utilities.
 */
object TestingResourceUtil {
  /**
   * Loads a [[com.moz.fiji.schema.layout.FijiTableLayout]] from the classpath. See
   * [[com.moz.fiji.schema.layout.FijiTableLayouts]] for some layouts that get put on the classpath
   * by FijiSchema.
   *
   * @param resourcePath Path to the layout definition file.
   * @return The layout contained within the provided resource.
   */
  def layout(resourcePath: String): FijiTableLayout = {
    val tableLayoutDef = FijiTableLayouts.getLayout(resourcePath)
    FijiTableLayout.newLayout(tableLayoutDef)
  }
}
