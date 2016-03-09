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

package com.moz.fiji.mapreduce.output;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.mapreduce.JobConfigurationException;
import com.moz.fiji.mapreduce.FijiTableContext;
import com.moz.fiji.mapreduce.framework.HFileKeyValue;
import com.moz.fiji.mapreduce.framework.FijiConfKeys;
import com.moz.fiji.mapreduce.impl.HFileWriterContext;
import com.moz.fiji.mapreduce.output.framework.FijiHFileOutputFormat;
import com.moz.fiji.mapreduce.platform.FijiMRPlatformBridge;
import com.moz.fiji.mapreduce.tools.framework.JobIOConfKeys;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiRegion;
import com.moz.fiji.schema.FijiRowKeySplitter;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.util.ResourceUtils;

/**
 * The class HFileMapReduceJobOutput is used to indicate the usage of HFiles based on the layout
 * of a Fiji table as output for a MapReduce job.
 *
 * <p>
 *   This job output writes output from MapReduce jobs to HFiles. The generated HFiles
 *   can be directly loaded into the regions of an existing HTable. Use a
 *   {@link com.moz.fiji.mapreduce.HFileLoader} to load HFiles into a Fiji table.
 * </p>
 *
 * <h2>Configuring an output:</h2>
 * <p>
 *   HFileMapReduceJobOutput must be configured with the address of the Fiji table to
 *   write to as well as a location to write output HFiles to:
 * </p>
 * <pre>
 *   <code>
 *     final FijiURI tableURI = FijiURI.newBuilder("fiji://.env/default/mytable").build();
 *     final Path hfileLocation = new Path("/path/to/hfile/output");
 *
 *     final MapReduceJobOutput fijiTableOutput =
 *         MapReduceJobOutputs.newHFileMapReduceJobOutput(tableURI, hfileLocation);
 *   </code>
 * </pre>
 * @see DirectFijiTableMapReduceJobOutput
 */
@ApiAudience.Public
@ApiStability.Evolving
public final class HFileMapReduceJobOutput extends FijiTableMapReduceJobOutput {
  private static final Logger LOG = LoggerFactory.getLogger(HFileMapReduceJobOutput.class);

  /**
   * If <code>mNumSplits</code> has this special value, the number of splits should be
   * set equal to the number of existing regions in the target Fiji table.
   */
  private static final int NUM_SPLITS_AUTO = 0;

  /** The path to the directory to create the HFiles in. */
  private Path mPath;

  /** Default constructor. Accessible via {@link MapReduceJobOutputs}. */
  HFileMapReduceJobOutput() {
  }
  /** {@inheritDoc} */
  @Override
  public void initialize(Map<String, String> params) throws IOException {
    super.initialize(params);
    mPath = new Path(params.get(JobIOConfKeys.FILE_PATH_KEY));
  }

  /**
   * Creates job output of HFiles that can be efficiently loaded into a Fiji table.
   * The number of HFiles created (which determines the number of reduce tasks) will
   * match the number of existing regions in the target Fiji table.
   *
   * @param tableURI The fiji table the resulting HFiles are intended for.
   * @param path The directory path to output the HFiles to.
   */
  HFileMapReduceJobOutput(FijiURI tableURI, Path path) {
    this(tableURI, path, NUM_SPLITS_AUTO);
  }

  /**
   * Creates job output of HFiles that can be efficiently loaded into a Fiji table.
   * The number of HFiles created (which determines the number of reduce tasks) is
   * specified with the <code>numSplits</code> argument.  Controlling the number of
   * splits is only possible when targeting a Fiji table with &lt;hashRowKeys&gt;
   * enabled.  Typically, you should allow the system to
   * match the number of splits to the number of regions in the table by using the
   * {@link #HFileMapReduceJobOutput(FijiURI, Path)} constructor instead.
   *
   * @param table The fiji table the resulting HFiles are intended for.
   * @param path The directory path to output the HFiles to.
   * @param numSplits Number of splits (determines the number of reduce tasks).
   */
  HFileMapReduceJobOutput(FijiURI table, Path path, int numSplits) {
    super(table, numSplits);
    mPath = path;
  }

  /** {@inheritDoc} */
  @Override
  public void configure(Job job) throws IOException {
    // sets Hadoop output format, Fiji output table and # of reducers:
    super.configure(job);

    final Configuration conf = job.getConfiguration();

    // Fiji table context:
    conf.setClass(
        FijiConfKeys.FIJI_TABLE_CONTEXT_CLASS,
        HFileWriterContext.class,
        FijiTableContext.class);

    // Set the output path.
    FileOutputFormat.setOutputPath(job, mPath);

    // Configure the total order partitioner so generated HFile shards are contiguous and sorted.
    configurePartitioner(job, makeTableKeySplit(getOutputTableURI(), getNumReduceTasks(), conf));

    // Note: the HFile job output requires the reducer of the MapReduce job to be IdentityReducer.
    //     This is enforced externally.
  }

  /** {@inheritDoc} */
  @Override
  protected Class<? extends OutputFormat> getOutputFormatClass() {
    return FijiHFileOutputFormat.class;
  }

  /**
   * Generates a split for a given table.
   *
   * @param tableURI URI of the Fiji table to split.
   * @param nsplits Number of splits.
   * @param conf Base Hadoop configuration used to open the Fiji instance.
   * @return a list of split start keys, as HFileKeyValue (with no value, just the keys).
   * @throws IOException on I/O error.
   */
  public static List<HFileKeyValue> makeTableKeySplit(FijiURI tableURI,
      int nsplits,
      Configuration conf)
      throws IOException {
    final Fiji fiji = Fiji.Factory.open(tableURI, conf);
    try {
      final FijiTable table = fiji.openTable(tableURI.getTable());
      try {
        if (NUM_SPLITS_AUTO == nsplits) {
          final List<HFileKeyValue> startKeys = Lists.newArrayList();
          for (FijiRegion region : table.getRegions()) {
            startKeys.add(HFileKeyValue.createFromRowKey(region.getStartKey()));
          }
          return startKeys;

        } else {
          switch (FijiTableLayout.getEncoding(table.getLayout().getDesc().getKeysFormat())) {
          case RAW: {
            // The user has explicitly specified how many HFiles to create, but this is not
            // possible when row key hashing is disabled.
            throw new JobConfigurationException(String.format(
                "Table '%s' has row key hashing disabled, so the number of HFile splits must be"
                + "determined by the number of HRegions in the HTable. "
                + "Use an HFileMapReduceJobOutput constructor that enables auto splitting.",
                table.getName()));
          }
          case FORMATTED:
          case HASH:
          case HASH_PREFIX: {
            // Those cases are supported:
            break;
          }
          default:
            throw new RuntimeException("Unhandled row key encoding: "
                + FijiTableLayout.getEncoding(table.getLayout().getDesc().getKeysFormat()));
          }
          return generateEvenStartKeys(nsplits);
        }
      } finally {
        ResourceUtils.releaseOrLog(table);
      }
    } finally {
      ResourceUtils.releaseOrLog(fiji);
    }
  }

  /** @return the path where to write HFiles. */
  public Path getPath() {
    return mPath;
  }

  /**
   * Configures the partitioner for generating HFiles.
   *
   * <p>Each generated HFile should fit within a region of of the target table.
   * Additionally, it's optimal to have only one HFile to load into each region, since a
   * read from that region will require reading from each HFile under management (until
   * compaction happens and merges them all back into one HFile).</p>
   *
   * <p>To achieve this, we configure a TotalOrderPartitioner that will partition the
   * records output from the Mapper based on their rank in a total ordering of the
   * keys.  The <code>startKeys</code> argument should contain a list of the first key in
   * each of those partitions.</p>
   *
   * @param job The job to configure.
   * @param startKeys A list of keys that will mark the boundaries between the partitions
   *     for the sorted map output records.
   * @throws IOException If there is an error.
   */
  public static void configurePartitioner(Job job, List<HFileKeyValue> startKeys)
      throws IOException {
    FijiMRPlatformBridge.get().setTotalOrderPartitionerClass(job);

    LOG.info("Configuring " + startKeys.size() + " reduce partitions.");
    job.setNumReduceTasks(startKeys.size());

    // Write the file that the TotalOrderPartitioner reads to determine where to partition records.
    Path partitionFilePath =
        new Path(job.getWorkingDirectory(), "partitions_" + System.currentTimeMillis());
    LOG.info("Writing partition information to " + partitionFilePath);

    final FileSystem fs = partitionFilePath.getFileSystem(job.getConfiguration());
    partitionFilePath = partitionFilePath.makeQualified(fs);
    writePartitionFile(job.getConfiguration(), partitionFilePath, startKeys);

    // Add it to the distributed cache.
    try {
      final URI cacheUri =
          new URI(partitionFilePath.toString() + "#" + TotalOrderPartitioner.DEFAULT_PATH);
      DistributedCache.addCacheFile(cacheUri, job.getConfiguration());
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
    DistributedCache.createSymlink(job.getConfiguration());
  }

  /**
   * <p>Write out a SequenceFile that can be read by TotalOrderPartitioner
   * that contains the split points in startKeys.</p>
   *
   * <p>This method was copied from HFileOutputFormat in hbase-0.90.1-cdh3u0.  I had to
   * copy it because it's private.</p>
   *
   * @param conf The job configuration.
   * @param partitionsPath output path for SequenceFile.
   * @param startKeys the region start keys to use as the partitions.
   * @throws IOException If there is an error.
   */
  public static void writePartitionFile(
      Configuration conf, Path partitionsPath, List<HFileKeyValue> startKeys)
      throws IOException {
    if (startKeys.isEmpty()) {
      throw new IllegalArgumentException("No regions passed");
    }

    // We're generating a list of split points, and we don't ever
    // have keys < the first region (which has an empty start key)
    // so we need to remove it. Otherwise we would end up with an
    // empty reducer with index 0.
    TreeSet<HFileKeyValue> sorted = new TreeSet<HFileKeyValue>();
    sorted.addAll(startKeys);

    HFileKeyValue first = sorted.first();
    if (0 != first.getRowKey().length) {
      throw new IllegalArgumentException(
          "First region of table should have empty start row key. Instead has: "
          + Bytes.toStringBinary(first.getRowKey()));
    }
    sorted.remove(first);

    // Write the actual file
    final SequenceFile.Writer writer = FijiMRPlatformBridge.get().newSeqFileWriter(
        conf, partitionsPath, HFileKeyValue.class, NullWritable.class);

    try {
      for (HFileKeyValue startKey : sorted) {
        writer.append(startKey, NullWritable.get());
      }
    } finally {
      writer.close();
    }
  }

  /**
   * <p>Generate a list of start keys (one per region).  Since we know
   * that the row keys in fiji are byte strings of length 16, we can reliably split
   * them evenly.</p>
   *
   * @param numRegions The number of regions to generate start keys for.
   * @return A list of start keys with size equal to <code>numRegions</code>.
   */
  private static List<HFileKeyValue> generateEvenStartKeys(int numRegions) {
    List<HFileKeyValue> startKeys = new ArrayList<HFileKeyValue>(numRegions);

    // The first key is a special case, it must be empty.
    startKeys.add(HFileKeyValue.createFromRowKey(HConstants.EMPTY_BYTE_ARRAY));

    if (numRegions > 1) {
      byte[][] splitKeys = FijiRowKeySplitter.get().getSplitKeys(numRegions);
      for (byte[] hbaseRowKey : splitKeys) {
        startKeys.add(HFileKeyValue.createFromRowKey(hbaseRowKey));
      }
    }
    return startKeys;
  }
}
