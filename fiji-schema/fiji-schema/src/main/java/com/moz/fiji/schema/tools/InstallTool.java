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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.common.flags.Flag;
import com.moz.fiji.schema.KConstants;
import com.moz.fiji.schema.FijiAlreadyExistsException;
import com.moz.fiji.schema.FijiInstaller;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.hbase.HBaseFactory;
import com.moz.fiji.schema.impl.hbase.HBaseSystemTable;

/**
 * A command-line tool for installing fiji instances on hbase clusters.
 */
@ApiAudience.Private
public final class InstallTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(InstallTool.class);

  @Flag(name="fiji", usage="URI of the Fiji instance to install.")
  private String mFijiURIFlag = KConstants.DEFAULT_INSTANCE_URI;

  @Flag(name="properties-file",
      usage="The properties file, if any, to use instead of the defaults.")
  private String mPropertiesFile = null;

  /** URI of the Fiji instance to install. */
  private FijiURI mFijiURI = null;

  /** Unmodifiable empty map. */
  private static final Map<String, String> EMPTY_MAP = Collections.emptyMap();

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "install";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Install a fiji instance onto a running HBase cluster.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Admin";
  }

  /** {@inheritDoc} */
  @Override
  protected void setup() throws Exception {
    super.setup();
    Preconditions.checkArgument((mFijiURIFlag != null) && !mFijiURIFlag.isEmpty(),
        "Specify the Fiji instance to install with --fiji=fiji://hbase-address/fiji-instance");
    mFijiURI = FijiURI.newBuilder(mFijiURIFlag).build();
    Preconditions.checkArgument(mFijiURI.getInstance() != null,
        "Specify the Fiji instance to install with --fiji=fiji://hbase-address/fiji-instance");
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    Preconditions.checkArgument(nonFlagArgs.isEmpty(),
        "Unexpected command-line argument: [%s]", Joiner.on(",").join(nonFlagArgs));
    getPrintStream().println("Creating fiji instance: " + mFijiURI);
    getPrintStream().println("Creating meta tables for fiji instance in hbase...");
    final Map<String, String> initialProperties = (null == mPropertiesFile)
            ? EMPTY_MAP
            : HBaseSystemTable.loadPropertiesFromFileToMap(mPropertiesFile);
    try {
      FijiInstaller.get().install(
          mFijiURI,
          HBaseFactory.Provider.get(),
          initialProperties,
          getConf());
      getPrintStream().println("Successfully created fiji instance: " + mFijiURI);
      return SUCCESS;
    } catch (FijiAlreadyExistsException kaee) {
      getPrintStream().printf("Fiji instance '%s' already exists.%n", mFijiURI);
      return FAILURE;
    }
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new FijiToolLauncher().run(new InstallTool(), args));
  }
}
