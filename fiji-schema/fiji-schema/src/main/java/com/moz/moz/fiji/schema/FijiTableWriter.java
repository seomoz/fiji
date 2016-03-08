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

package com.moz.fiji.schema;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.annotations.Inheritance;

/**
 * Interface for modifying a Fiji table.
 *
 * <p>
 *   Wraps methods from FijiPutter, FijiIncrementer, and FijiDeleter.
 *   To get a FijiTableWriter, use {@link com.moz.fiji.schema.FijiTable#openTableWriter()}
 *   or {@link com.moz.fiji.schema.FijiTable#getWriterFactory()}.
 * </p>
 *
 * <p>
 *   Unless otherwise specified, writers are not thread-safe and must be synchronized externally.
 * </p>
 */
@ApiAudience.Public
@ApiStability.Evolving
@Inheritance.Sealed
public interface FijiTableWriter extends FijiPutter, FijiIncrementer, FijiDeleter {
}
