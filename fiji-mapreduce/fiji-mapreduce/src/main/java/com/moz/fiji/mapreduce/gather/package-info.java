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
 * Gatherer classes for FijiMR clients.
 *
 * <p>
 *   A Fiji Gatherer scans over the rows of a Fiji table using the MapReduce framework to
 *   aggregate information which can be passed to a reducer.
 *   The {@link com.moz.fiji.mapreduce.gather.FijiGatherer} class is the base class for all gatherers.
 *   Subclasses take inputs from {@link com.moz.fiji.schema.FijiTable} and produce output to
 *   to HFiles.
 * </p>
 *
 * <h2>Constructing a gatherer job:</h2>
 * <p>
 *   A gatherer job that outputs to HFiles can be created here:
 * </p>
 * <pre><code>
 *   // Configure and create the MapReduce job.
 *   final MapReduceJob job = FijiGatherJobBuilder.create()
 *       .withConf(conf)
 *       .withInputTable(mTable)
 *       .withGatherer(SimpleGatherer.class)
 *       .withCombiner(MyCombiner.class)
 *       .withReducer(MyReducer.class)
 *       .withOutput(new TextMapReduceJobOutput(new Path("mypath"), 10))
 *       .build();
 * </code></pre>
 * <p>
 *   The <code>fiji gather</code> command line tool wraps this functionality and can be used
 *   for constructing gathererjobs.
 * </p>
 */

package com.moz.fiji.mapreduce.gather;
