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
 * FijiMR utilities.
 *
 * <p>
 *   FijiMR includes APIs to build MapReduce jobs that run over Fiji tables, bringing
 *   MapReduce-based analytic techniques to a broad base of Fiji Schema users.
 * </p>
 *
 * <h2>Building MapReduce jobs:</h2>
 * <p>
 *   FijiMR contains many job builders for various types of MapReduce jobs.
 * </p>
 * <ul>
 * <li>{@link com.moz.fiji.mapreduce.bulkimport Bulk Importers} - for the creation of bulk
 *     import jobs which allow data to be inserted into Fiji tables efficiently.</li>
 * <li>{@link com.moz.fiji.mapreduce.produce Producers} - for the creation of produce jobs
 *     which generate per-row derived entity data.<!--  -->
 * <li>{@link com.moz.fiji.mapreduce.gather Gatherers} - for the creation of gather jobs
 *     which scan over the rows of a Fiji table to aggregate information which can be passed to a
 *     reducer.<!--  -->
 * <li>{@link com.moz.fiji.mapreduce.FijiMapReduceJobBuilder General MapReduce} - for the creation of
 *     general MapReduce jobs around Fiji mappers and reducers.</li>
 * </ul>
 *
 * <h2>Utility packages for MapReduce job construction:</h2>
 * <ul>
 * <li>{@link com.moz.fiji.mapreduce.input} - input formats for MapReduce jobs</li>
 * <li>{@link com.moz.fiji.mapreduce.output} - output formats for MapReduce jobs</li>
 * <li>{@link com.moz.fiji.mapreduce.kvstore} - Key-Value store API used in MapReduce jobs</li>
 * </ul>
 */
package com.moz.fiji.mapreduce;
