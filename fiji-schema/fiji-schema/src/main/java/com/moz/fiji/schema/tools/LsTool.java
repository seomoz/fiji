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
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.KConstants;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.hbase.HBaseFactory;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayout.LocalityGroupLayout.FamilyLayout;
import com.moz.fiji.schema.layout.FijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;
import com.moz.fiji.schema.util.ResourceUtils;

/**
 * Command-line tool to explore fiji table data like the 'ls' command of a unix shell.
 *
 * List all fiji instances:
 * <pre>
 *   fiji ls
 *   fiji ls fiji://.env
 *   fiji ls fiji://localhost:2181
 *   fiji ls fiji://{host1,host2}:2181
 * </pre>
 *
 * List all fiji tables:
 * <pre>
 *   fiji ls default
 *   fiji ls fiji://.env/default
 * </pre>
 *
 * List all columns in a fiji table 'table_foo':
 * <pre>
 *   fiji ls default/table_foo
 *   fiji ls fiji://.env/default/table_foo
 * </pre>
 *
 */
@ApiAudience.Private
public final class LsTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(LsTool.class);

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "ls";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "List Fiji instances, tables and columns.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Data";
  }

  /** {@inheritDoc} */
  @Override
  public String getUsageString() {
    return
        "Usage:\n"
        + "    fiji ls [flags...] [<fiji-uri>...] \n"
        + "\n"
        + "Example:\n"
        + "  Listing the Fiji instances from the default HBase cluster:\n"
        + "    fiji ls\n"
        + "    fiji ls fiji://.env\n"
        + "\n"
        + "  Listing the Fiji tables from the Fiji instance named 'default':\n"
        + "    fiji ls default\n"
        + "    fiji ls fiji://.env/default\n"
        + "\n"
        + "  Listing the columns in the Fiji table 'table':\n"
        + "    fiji ls default/table\n"
        + "    fiji ls fiji://.env/default/table\n"
        + "    fiji ls fiji://localhost:2181/default/table\n";
  }

  /**
   * Lists all fiji instances.
   *
   * @param hbaseURI URI of the HBase instance to list the content of.
   * @return A program exit code (zero on success).
   * @throws IOException If there is an error.
   */
  private int listInstances(FijiURI hbaseURI) throws IOException {
    for (String instanceName : getInstanceNames(hbaseURI)) {
      getPrintStream().println(FijiURI.newBuilder(hbaseURI).withInstanceName(instanceName).build());
    }
    return SUCCESS;
  }

  /**
   * Returns a set of instance names.
   *
   * @param hbaseURI URI of the HBase instance to list the content of.
   * @return ordered set of instance names.
   * @throws IOException on I/O error.
   */
  protected static Set<String> getInstanceNames(FijiURI hbaseURI) throws IOException {
    // TODO(SCHEMA-188): Consolidate this logic in a single central place:
    final Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.ZOOKEEPER_QUORUM,
        Joiner.on(",").join(hbaseURI.getZookeeperQuorumOrdered()));
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, hbaseURI.getZookeeperClientPort());
    final HBaseAdmin hbaseAdmin =
        HBaseFactory.Provider.get().getHBaseAdminFactory(hbaseURI).create(conf);

    try {
      final Set<String> instanceNames = Sets.newTreeSet();
      for (HTableDescriptor hTableDescriptor : hbaseAdmin.listTables()) {
        final String instanceName = parseInstanceName(hTableDescriptor.getNameAsString());
        if (null != instanceName) {
          instanceNames.add(instanceName);
        }
      }
      return instanceNames;
    } finally {
      ResourceUtils.closeOrLog(hbaseAdmin);
    }
  }

  /**
   * Parses a table name for a fiji instance name.
   *
   * @param fijiTableName The table name to parse
   * @return instance name (or null if none found)
   */
  protected static String parseInstanceName(String fijiTableName) {
    final String[] parts = StringUtils.split(fijiTableName, '\u0000', '.');
    if (parts.length < 3 || !FijiURI.FIJI_SCHEME.equals(parts[0])) {
      return null;
    }
    return parts[1];
  }

  /**
   * Lists all the tables in a fiji instance.
   *
   * @param fiji Fiji instance to list the tables of.
   * @return A program exit code (zero on success).
   * @throws IOException If there is an error.
   */
  private int listTables(Fiji fiji) throws IOException {
    for (String name : fiji.getTableNames()) {
      getPrintStream().println(fiji.getURI() + name);
    }
    return SUCCESS;
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    if (nonFlagArgs.isEmpty()) {
      nonFlagArgs.add(KConstants.DEFAULT_HBASE_URI);
    }

    int status = SUCCESS;
    for (String arg : nonFlagArgs) {
      status = (run(FijiURI.newBuilder(arg).build()) == SUCCESS) ? status : FAILURE;
    }
    return status;
  }

  /**
   * Lists instances, tables, or columns in a fiji URI.
   * Can be recursively called by run(List<String>).
   *
   * @param argURI Fiji URI from which to list instances, tables, or columns.
   * @return A program exit code (zero on success).
   * @throws Exception If there is an error.
   */
  private int run(final FijiURI argURI) throws Exception {
    if (argURI.getZookeeperQuorum() == null) {
      getPrintStream().printf("Specify a cluster with argument: fiji://zookeeper-quorum%n");
      return FAILURE;
    }

    if (argURI.getInstance() == null) {
      // List instances in this fiji instance.
      return listInstances(argURI);
    }

    final Fiji fiji = Fiji.Factory.open(argURI, getConf());
    try {
      if (argURI.getTable() == null) {
        // List tables in this fiji instance.
        return listTables(fiji);
      }

      final FijiTable table = fiji.openTable(argURI.getTable());
      try {
        final FijiTableLayout tableLayout = table.getLayout();
        for (FamilyLayout family : tableLayout.getFamilies()) {
          if (family.isMapType()) {
            getPrintStream().println(FijiURI.newBuilder(table.getURI())
                .addColumnName(FijiColumnName.create(family.getName()))
                .build());
          } else {
            for (ColumnLayout column : family.getColumns()) {
              getPrintStream().println(FijiURI.newBuilder(table.getURI())
                  .addColumnName(FijiColumnName.create(family.getName(), column.getName()))
                  .build());
            }
          }
        }
        return SUCCESS;
      } finally {
        ResourceUtils.releaseOrLog(table);
      }
    } finally {
      ResourceUtils.releaseOrLog(fiji);
    }
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new FijiToolLauncher().run(new LsTool(), args));
  }
}
