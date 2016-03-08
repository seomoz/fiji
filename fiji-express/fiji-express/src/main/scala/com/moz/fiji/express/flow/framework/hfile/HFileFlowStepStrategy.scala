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

package com.moz.fiji.express.flow.framework.hfile

import java.net.URI
import java.util.{List => JList}

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

import cascading.flow.Flow
import cascading.flow.FlowStep
import cascading.flow.FlowStepStrategy
import org.apache.hadoop.mapreduce.filecache.DistributedCache
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.lib.IdentityReducer
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner
import org.apache.hadoop.mapreduce.lib.partition.{
    TotalOrderPartitioner => TotalOrderPartitionerMR2 }

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.annotations.Inheritance
import com.moz.fiji.mapreduce.framework.HFileKeyValue
import com.moz.fiji.mapreduce.framework.HFileKeyValue.FastComparator
import com.moz.fiji.mapreduce.output.HFileMapReduceJobOutput
import com.moz.fiji.schema.FijiURI

/**
 * An implementation of a Cascading FlowStepStrategy used to alter the properties
 * of the flow step corresponding to the sink to support writing directly to HFiles. This
 * will only operate on FlowSteps where the sink's outputFormat is the FijiHFileOutputFormat.
 *
 * There are two situations that can happen when writing to HFiles:
 * <ol>
 *  <li> The Cascading sink step is a map-only flow (with no reducer). In this case, the Identity
 *  reducer will be forced to be used and the correct partitioner configured so that the
 *  tuples will be sinked to HFiles. </li>
 *  <li> The sink step is part of an flow with a reducer in which case the output will be routed
 *  to a temporary sequence file that a secondary M/R job will use to correct sort and store
 *  the data into HFiles for bulk loading </li>
 * </ol>
 *
 */
@ApiAudience.Framework
@ApiStability.Stable
@Inheritance.Sealed
object HFileFlowStepStrategy extends FlowStepStrategy[JobConf] {

  override def apply(
      flow: Flow[JobConf],
      predecessorSteps: JList[FlowStep[JobConf]],
      flowStep: FlowStep[JobConf]) {
    val hfileSinks = flowStep.getSinks.asScala.collect { case sink: HFileFijiTap => sink }
    if (hfileSinks.nonEmpty) {
      val sink = hfileSinks.head // should only be 1 element
      val conf = flowStep.getConfig

      conf.setPartitionerClass(classOf[TotalOrderPartitioner[HFileKeyValue, NullWritable]])
      conf.setReducerClass(classOf[IdentityReducer[HFileKeyValue, NullWritable]])
      conf.setMapOutputKeyClass(classOf[HFileKeyValue])
      conf.setMapOutputValueClass(classOf[NullWritable])
      conf.setOutputKeyComparatorClass(classOf[FastComparator])

      val fijiURI = FijiURI.newBuilder(sink.tableUri).build()
      val splits = HFileMapReduceJobOutput.makeTableKeySplit(fijiURI, 0, conf)
      if (splits.size > 1) {
        val mr1Mode = Option(conf.get("mapred.job.tracker"))
        val yarnMode = Option(conf.get("mapreduce.framework.name"))
        val mode = yarnMode orElse mr1Mode orElse Option("local")
        require(
          mode.get != "local",
          "Cannot create HFiles for table with more than a single region in local map reduce mode."
        )
      }
      conf.setNumReduceTasks(splits.size())

      // Write the partition file for the TotalOrderPartitioner
      val partitionPath = {
        // For some reason, TotalOrderPartitioner.DEFAULT_PATH does not compile,
        // even though TotalOrderPartitioner inherits from TotalOrderPartitionerMR2,
        // so we had to import TotalPartitionerMR2 separately.
        val path = new Path(sink.hFileOutput, TotalOrderPartitionerMR2.DEFAULT_PATH)
        path.getFileSystem(conf).makeQualified(path)
      }
      HFileMapReduceJobOutput.writePartitionFile(conf, partitionPath, splits)
      conf.set("total.order.partitioner.path", partitionPath.toString)
      val cacheUri = new URI(partitionPath.toString + "#" + TotalOrderPartitionerMR2.DEFAULT_PATH)
      DistributedCache.addCacheFile(cacheUri, conf)
      DistributedCache.createSymlink(conf)
    }
  }
}
