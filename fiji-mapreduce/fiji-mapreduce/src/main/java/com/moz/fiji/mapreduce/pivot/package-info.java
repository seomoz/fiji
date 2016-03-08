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
 * Pivot classes for FijiMR clients.
 *
 * <p>
 *   A {@link com.moz.fiji.mapreduce.pivot.FijiPivoter} scans over the rows of an input Fiji table and
 *   writes cells into an output Fiji table which may or may not be the same table. The
 *   {@link com.moz.fiji.mapreduce.pivot.FijiPivoter} class is the base class for all pivoter job.
 *   Conceptually, a {@link com.moz.fiji.mapreduce.pivot.FijiPivoter} is a map-only job that reads from
 *   a Fiji table and writes to a Fiji table.
 * </p>
 *
 * <h2>Constructing a pivot job:</h2>
 * <p> A pivot job that writes HFiles can be created as follows: </p>
 * <pre><blockquote>
 *   final Configuration conf = ...;
 *   final FijiURI inputTableURI = ...;
 *   final FijiURI outputTableURI = ...;
 *   final FijiMapReduceJob job = FijiPivotJobBuilder.create()
 *       .withConf(conf)
 *       .withPivoter(SomePivoter.class)
 *       .withInputTable(inputTableURI)
 *       .withOutput(MapReduceJobOutputs
 *           .newHFileMapReduceJobOutput(outputTableURI, hfilePath))
 *       .build();
 *   job.run();
 * </blockquote></pre>
 * <p>
 *   The {@code fiji pivot} command line tool wraps this functionality and can be used
 *   to launch pivot jobs.
 * </p>
 */

package com.moz.fiji.mapreduce.pivot;
