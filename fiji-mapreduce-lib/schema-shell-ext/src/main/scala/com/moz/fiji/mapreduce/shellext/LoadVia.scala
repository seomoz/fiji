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

package com.moz.fiji.mapreduce.shellext

import com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HConstants;

import com.moz.fiji.mapreduce.HFileLoader;
import com.moz.fiji.mapreduce.MapReduceJobInput
import com.moz.fiji.mapreduce.MapReduceJobOutput
import com.moz.fiji.mapreduce.output.MapReduceJobOutputs
import com.moz.fiji.schema.Fiji
import com.moz.fiji.schema.FijiTable
import com.moz.fiji.schema.FijiURI
import com.moz.fiji.schema.shell.DDLException
import com.moz.fiji.schema.shell.ddl.ColumnName
import com.moz.fiji.schema.util.ResourceUtils;

/**
 * A clause specifying how the data is loaded into the target table: directly,
 * or via an intermediate HDFS path.
 */
abstract class LoadVia {
  /**
   * Create a MapReduceJobOutput that writes to the specified table.
   *
   * @param tableURI the Fiji URI specifying the instance and table to write to.
   * @return the MapReduceJobOutput describing the import target.
   */
  def outputToTable(tableURI: FijiURI): MapReduceJobOutput

  /**
   * Run any bulk load operation, if necessary, to complete the import process.
   *
   * @param conf the hadoop Configuration used to run the bulk import.
   * @param tableURI the FijiURI specifying the instance and table to write to.
   */
  def runBulkLoad(conf: Configuration, tableURI: FijiURI): Unit
}

/**
 * <tt>DIRECT</tt> mechanism for puts.
 */
class LoadViaDirect extends LoadVia {
  /** {@inheritDoc} */
  override def outputToTable(tableURI: FijiURI): MapReduceJobOutput = {
    return MapReduceJobOutputs.newDirectFijiTableMapReduceJobOutput(tableURI)
  }

  /** {@inheritDoc} */
  override def runBulkLoad(conf: Configuration, tableURI: FijiURI): Unit = { /* nothing to do */ }
}

/**
 * <tt>LOAD THROUGH 'path'</tt> version of a via_clause.
 */
class LoadViaPath(val hdfsUri: String) extends LoadVia {

  /** HFIleMapReduceJobOutput will calculate the split count if nSplits=0. */
  private val AUTO_NUM_SPLITS = 0

  /** {@inheritDoc} */
  override def outputToTable(tableURI: FijiURI): MapReduceJobOutput = {
    return MapReduceJobOutputs.newHFileMapReduceJobOutput(
        tableURI, new Path(hdfsUri), AUTO_NUM_SPLITS)
  }

  /** {@inheritDoc} */
  override def runBulkLoad(conf: Configuration, tableURI: FijiURI): Unit = {

    val fiji: Fiji = Fiji.Factory.open(tableURI)
    try {
      val table: FijiTable = fiji.openTable(tableURI.getTable())
      try {
        // Load the HFiles
        val fs: FileSystem = FileSystem.get(conf)
        val rootPath: Path = new Path(hdfsUri)
        conf.set(HConstants.ZOOKEEPER_QUORUM,
            Joiner.on(",").join(tableURI.getZookeeperQuorumOrdered()))
        conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, tableURI.getZookeeperClientPort())

        // Get all subdirs one level down from the output directory from the job;
        // load them all individually. Each reducer can create several sets of files.
        val loader = HFileLoader.create(conf)
        val files: Array[FileStatus] = fs.listStatus(rootPath)
        if (null == files) {
          throw new DDLException("Error: The bulk import job didn't create any files!")
        }

        files.foreach { fileStatus: FileStatus =>
          if (fileStatus.isDirectory()) {
            val path = fileStatus.getPath()
            if (!path.getName().startsWith("_")) {
              // If it's a dir and it isn't "hidden" like _logs, bulk load it.
              loader.load(fileStatus.getPath(), table)
            }
          }
        }

        // Delete the dir they lived in.
        fs.delete(rootPath, true)
      } finally {
        ResourceUtils.releaseOrLog(table);
      }
    } finally {
      ResourceUtils.releaseOrLog(fiji);
    }
  }
}
