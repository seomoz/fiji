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

import java.util.List;

import com.google.common.base.Preconditions;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.common.flags.Flag;
import com.moz.fiji.schema.KConstants;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.util.ProtocolVersion;
import com.moz.fiji.schema.util.VersionInfo;

/**
 * Command-line tool for displaying the fiji software version running and the fiji data version
 * in use for a specified fiji instance.
 */
@ApiAudience.Private
public final class VersionTool extends BaseTool {

  @Flag(name="fiji", usage="URI of the Fiji instance to print the version of.")
  private String mFijiURIFlag = KConstants.DEFAULT_INSTANCE_URI;

  private FijiURI mFijiURI = null;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "version";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Print the fiji distribution and data versions in use.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Help";
  }

  /** {@inheritDoc} */
  @Override
  protected void setup() throws Exception {
    Preconditions.checkArgument((mFijiURIFlag != null) && !mFijiURIFlag.isEmpty(),
        "Specify the Fiji instance to uninstall with --fiji=fiji://hbase-address/fiji-instance");
    mFijiURI = FijiURI.newBuilder(mFijiURIFlag).build();
    Preconditions.checkArgument(mFijiURI.getInstance() != null,
        "Specify the Fiji instance to uninstall with --fiji=fiji://hbase-address/fiji-instance");
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    final String clientSoftwareVersion = VersionInfo.getSoftwareVersion();
    getPrintStream().println("fiji client software version: " + clientSoftwareVersion);

    final ProtocolVersion clientDataVersion = VersionInfo.getClientDataVersion();
    getPrintStream().println("fiji client data version: " + clientDataVersion);

    final Fiji fiji = Fiji.Factory.open(mFijiURI, getConf());
    try {
      final ProtocolVersion clusterDataVersion = VersionInfo.getClusterDataVersion(fiji);
      getPrintStream().println("fiji cluster data version: " + clusterDataVersion);
    } finally {
      fiji.release();
    }

    ProtocolVersion minimumLayoutVersion = FijiTableLayout.getMinSupportedLayoutVersion();
    ProtocolVersion maximumLayoutVersion = FijiTableLayout.getMaxSupportedLayoutVersion();
    getPrintStream().println("layout versions supported: "
        + minimumLayoutVersion + " to " + maximumLayoutVersion);

    return SUCCESS;
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new FijiToolLauncher().run(new VersionTool(), args));
  }
}
