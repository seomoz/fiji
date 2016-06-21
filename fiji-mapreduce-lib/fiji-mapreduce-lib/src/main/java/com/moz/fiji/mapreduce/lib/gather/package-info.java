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

/**
 * Gatherers for FijiMR.
 *
 * <p>
 *   A Fiji Gatherer scans over the rows of a Fiji table using the MapReduce framework to
 *   aggregate information which can be passed to a Reducer.  Gather jobs in FijiMR can be
 *   created using the <code>FijiGatherJobBuilder</code>. Gather jobs are invoked
 *   using the <code>fiji gather</code> tool.
 * </p>
 *
 * <h2>Usable gatherers:</h2>
 * <ul>
 * <li>{@link com.moz.fiji.mapreduce.lib.gather.MapTypeDelimitedFileGatherer} - Gatherer that flattens
 *     map-type Fiji data into delimited files in HDFS.</li>
 * </ul>
 */
package com.moz.fiji.mapreduce.lib.gather;
