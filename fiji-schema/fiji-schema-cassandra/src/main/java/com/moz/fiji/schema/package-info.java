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
 * The main package for users of FijiSchema.
 *
 * <p>
 * FijiSchema provides layout and schema management on top of HBase.  FijiSchema also allows
 * for the use of both structured and unstructured data in HBase using Avro serialization.
 * </p>
 *
 * <p>
 *   Classes of note:
 * </p>
 * <ul>
 *   <li>{@link com.moz.fiji.schema.Fiji Fiji} - Root class representing a Fiji instance which
 *       provides access to FijiTables.</li>
 *   <li>{@link com.moz.fiji.schema.FijiTable FijiTable} - provides access to
 *       {@link com.moz.fiji.schema.FijiTableReader FijiTableReader} and
 *       {@link com.moz.fiji.schema.FijiTableWriter FijiTableWriter}</li>
 *   <li>{@link com.moz.fiji.schema.FijiTablePool FijiTablePool} - maintains a shared pool of
 *       connections to FijiTables.</li>
 *   <li>{@link com.moz.fiji.schema.FijiDataRequest FijiDataRequest} - a data request for column data
 *       from FijiTables.</li>
 *   <li>{@link com.moz.fiji.schema.FijiRowData FijiRowData} - data for a single entity.</li>
 *   <li>{@link com.moz.fiji.schema.EntityIdFactory EntityIdFactory} - factory for
 *       {@link com.moz.fiji.schema.EntityId}s which identify particular rows in FijiTables. </li>
 * </ul>
 *
 * <p>
 *   Related Documentation:
 * </p>
 * <ul>
 *   <li><a target="_top" href=http;//docs.fiji.org/tutorials>
 *     Tutorials</a></li>
 *   <li><a target="_top" href=http://docs.fiji.org/userguide>
 *     User Guide</a></li>
 * </ul>
 */
package com.moz.fiji.schema;
