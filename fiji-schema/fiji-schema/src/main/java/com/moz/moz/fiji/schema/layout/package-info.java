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

/**
 * Fiji table layouts.
 *
 * <p>
 * A Fiji table has a <i>layout</i> that describes the locality groups, families, and
 * columns it contains.
 *
 * This package contains code for serializing the layout metadata to and from JSON or binary Avro
 * records, and persisting it into the Fiji system tables in HBase.
 *
 * Fiji table layouts are persisted in the {@link com.moz.fiji.schema.FijiMetaTable}.
 * Fiji records the history of the layouts of each table.
 * When updating the layout of a table, Fiji validates the new layout against the current layout
 * of the table.
 *
 * Fiji table layouts are serialized to descriptors as {@link com.moz.fiji.schema.avro.TableLayoutDesc}
 * Avro records (see the Avro record definitions in {@code src/main/avro/Layout.avdl}).
 *
 * Fiji table layouts are internally represented as {@link com.moz.fiji.schema.layout.FijiTableLayout}
 * instances. {@link com.moz.fiji.schema.layout.FijiTableLayout FijiTableLayout} wraps layout
 * descriptors, validate layout updates, assigns column IDs and provides convenience accessors.
 * </p>
 */
package com.moz.fiji.schema.layout;
