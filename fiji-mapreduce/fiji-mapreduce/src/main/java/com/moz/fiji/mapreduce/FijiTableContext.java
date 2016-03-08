/**
 * (c) Copyright 2012 WibiData, Inc.
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

package com.moz.fiji.mapreduce;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.annotations.Inheritance;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.EntityIdFactory;
import com.moz.fiji.schema.FijiDeleter;
import com.moz.fiji.schema.FijiPutter;

/** Context for Fiji bulk-importers or reducers to output to a Fiji table. */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Sealed
public interface FijiTableContext extends FijiContext, FijiPutter, FijiDeleter {
  /** @return a factory to create entity IDs to write to the output Fiji table. */
  EntityIdFactory getEntityIdFactory();

  /**
   * Creates an entity ID for the specified Fiji row key.
   *
   * @param components Fiji row key components.
   * @return the entity ID for the specified Fiji row key.
   */
  EntityId getEntityId(Object... components);
}
