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

package com.moz.fiji.schema.tools;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.common.flags.Flag;
import com.moz.fiji.schema.KConstants;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.hbase.FijiManagedHBaseTableName;
import com.moz.fiji.schema.util.ResourceUtils;

/**
 * Command-line tool for flushing Fiji meta and user tables in HBase.
 *
 * <h2>Examples:</h2>
 * Flush a Fiji table:
 * <pre>
 *   fiji flush-table --target=fiji://my-hbase/my-instance/my-table/
 * </pre>
 * Flush all meta tables in an instance:
 * <pre>
 *   fiji flush-table --target=fiji://my-hbase/my-instance/ --meta=true
 * </pre>
 */
@ApiAudience.Private
public final class FlushTableTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(FlushTableTool.class.getName());

  @Flag(name="target", usage="URI of the Fiji table or the Fiji instance to flush.")
  private String mTargetURIFlag = KConstants.DEFAULT_INSTANCE_URI;

  @Flag(name="meta", usage="If true, flushes all fiji meta tables.")
  private boolean mFlushMeta = false;

  private HBaseAdmin mHBaseAdmin;

  /** URI of the Fiji table or the Fiji instance to flush. */
  private FijiURI mTargetURI;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "flush-table";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Flush fiji user and meta table write-ahead logs.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Admin";
  }

  /** {@inheritDoc} */
  @Override
  protected void validateFlags() throws Exception {
    super.validateFlags();
    Preconditions.checkArgument((mTargetURIFlag != null) && !mTargetURIFlag.isEmpty(),
        "Specify a target Fiji instance or table "
        + "with --target=fiji://hbase-adress/fiji-instance[/table].");
    mTargetURI = FijiURI.newBuilder(mTargetURIFlag).build();

    Preconditions.checkArgument(mFlushMeta || (mTargetURI.getTable() != null),
        "Specify a table with --fiji=fiji://hbase-cluster/fiji-instance/table"
        + " and/or specify a flush of metadata with --meta.");
  }

  /**
   * Flushes all metadata tables.
   *
   * @param hbaseAdmin An hbase admin utility.
   * @param instanceName The name of the Fiji instance.
   * @throws IOException If there is an error.
   * @throws InterruptedException If the thread is interrupted.
   */
  private void flushMetaTables(HBaseAdmin hbaseAdmin, String instanceName)
      throws IOException, InterruptedException {
    LOG.debug("Flushing schema hash table");
    FijiManagedHBaseTableName hbaseTableName = FijiManagedHBaseTableName.getSchemaHashTableName(
        instanceName);
    hbaseAdmin.flush(hbaseTableName.toString());

    LOG.debug("Flushing schema id table");
    hbaseTableName = FijiManagedHBaseTableName.getSchemaIdTableName(instanceName);
    hbaseAdmin.flush(hbaseTableName.toString());

    LOG.debug("Flushing meta table");
    hbaseTableName = FijiManagedHBaseTableName.getMetaTableName(
        instanceName);
    hbaseAdmin.flush(hbaseTableName.toString());

    LOG.debug("Flushing system table");
    hbaseTableName = FijiManagedHBaseTableName.getSystemTableName(instanceName);
    hbaseAdmin.flush(hbaseTableName.toString());

    LOG.debug("Flushing -ROOT-");
    hbaseAdmin.flush("-ROOT-");

    LOG.debug("Flushing .META.");
    hbaseAdmin.flush(".META.");
  }

  /**
   * Flushes a fiji table with the name 'tableName'.
   *
   * @param hbaseAdmin An hbase admin utility.
   * @param tableURI URI of the Fiji table to flush.
   * @throws IOException If there is an error.
   * @throws InterruptedException If the thread is interrupted.
   */
  private static void flushTable(HBaseAdmin hbaseAdmin, FijiURI tableURI)
      throws IOException, InterruptedException {
    final FijiManagedHBaseTableName hbaseTableName =
        FijiManagedHBaseTableName.getFijiTableName(tableURI.getInstance(), tableURI.getTable());
    hbaseAdmin.flush(hbaseTableName.toString());
  }

  /** {@inheritDoc} */
  @Override
  protected void setup() throws Exception {
    super.setup();
    getConf().setInt(HConstants.ZOOKEEPER_CLIENT_PORT, mTargetURI.getZookeeperClientPort());
    getConf().set(HConstants.ZOOKEEPER_QUORUM,
        Joiner.on(",").join(mTargetURI.getZookeeperQuorumOrdered()));
    setConf(HBaseConfiguration.addHbaseResources(getConf()));
    mHBaseAdmin = new HBaseAdmin(getConf());
  }

  /** {@inheritDoc} */
  @Override
  protected void cleanup() throws IOException {
    ResourceUtils.closeOrLog(mHBaseAdmin);
    super.cleanup();
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    if (mFlushMeta) {
      getPrintStream().println("Flushing metadata tables for fiji instance: "
          + mTargetURI.toString());
      flushMetaTables(mHBaseAdmin, mTargetURI.getInstance());
    }

    if (null != mTargetURI) {
      getPrintStream().printf("Flushing table '%s'.%n", mTargetURI);
      flushTable(mHBaseAdmin, mTargetURI);
    }

    getPrintStream().println("Flush operations successfully enqueued.");

    return 0;
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new FijiToolLauncher().run(new FlushTableTool(), args));
  }
}
