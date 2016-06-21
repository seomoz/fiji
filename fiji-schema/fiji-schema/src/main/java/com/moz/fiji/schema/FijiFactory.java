/**
 * (c) Copyright 2013 WibiData, Inc.
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

package com.moz.fiji.schema;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.annotations.Inheritance;
import com.moz.fiji.delegation.PriorityProvider;

/**
 * Factory for Fiji instances.
 *
 * <p>
 *   <em>Note:</em> the {@link #getPriority} method inherited from {@link PriorityProvider} should
 *   not be used. This method is considered deprecated. Instead, use
 *   {@link com.moz.fiji.schema.FijiURI#getFijiFactory()}.
 * </p>
 */
@ApiAudience.Public
@ApiStability.Evolving
@Inheritance.Sealed
public interface FijiFactory extends /* @Deprecated */ PriorityProvider {
  /**
   * Opens a Fiji instance by URI.
   *
   * @param uri URI specifying the Fiji instance to open.
   * @return the specified Fiji instance.
   * @throws IOException on I/O error.
   */
  Fiji open(FijiURI uri) throws IOException;

  /**
   * Opens a Fiji instance by URI.
   *
   * @param uri URI specifying the Fiji instance to open.
   * @param conf Hadoop configuration.
   * @return the specified Fiji instance.
   * @throws IOException on I/O error.
   */
  Fiji open(FijiURI uri, Configuration conf) throws IOException;
}
