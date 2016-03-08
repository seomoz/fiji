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

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.common.flags.Flag;
import com.moz.fiji.schema.KConstants;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiInstaller;
import com.moz.fiji.schema.FijiInvalidNameException;
import com.moz.fiji.schema.FijiNotInstalledException;
import com.moz.fiji.schema.FijiURI;

/**
 * A command-line tool for uninstalling fiji instances from an hbase cluster.
 */
@ApiAudience.Private
public final class UninstallTool extends BaseTool {

  @Flag(name="fiji", usage="URI of the Fiji instance to uninstall.")
  private String mFijiURIFlag = KConstants.DEFAULT_INSTANCE_URI;

  /** URI of the Fiji instance to uninstall. */
  private FijiURI mFijiURI = null;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "uninstall";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Remove a fiji instance from a running HBase cluster.";
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
        "Specify the Fiji instance to uninstall with --fiji=fiji://hbase-address/fiji-instance");
    mFijiURI = FijiURI.newBuilder(mFijiURIFlag).build();
    Preconditions.checkArgument(mFijiURI.getInstance() != null,
        "Specify the Fiji instance to uninstall with --fiji=fiji://hbase-address/fiji-instance");
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    Preconditions.checkArgument(nonFlagArgs.isEmpty(),
        "Unexpected command-line argument: [%s]", Joiner.on(",").join(nonFlagArgs));
    getPrintStream().println("Deleting fiji instance: " + mFijiURI.toString());
    if (isInteractive())  {
      final Fiji fiji = Fiji.Factory.open(mFijiURI, getConf());
      try {
        getPrintStream().println("WARNING: This instance contains the table(s):");
        for (String name : fiji.getTableNames()) {
          getPrintStream().println(name);
        }
      } finally {
        fiji.release();
      }

      getPrintStream().println();
      if (!inputConfirmation("Are you sure? This action will delete all meta and user data "
          + "from hbase and cannot be undone!", mFijiURI.getInstance())) {
        getPrintStream().println("Delete aborted.");
        return FAILURE;
      }
    }
    try {
      FijiInstaller.get().uninstall(mFijiURI, getConf());
      getPrintStream().println("Deleted fiji instance: " + mFijiURI.toString());
      return SUCCESS;
    } catch (IOException ioe) {
      getPrintStream().println("Error performing I/O during uninstall: " + ioe.getMessage());
      return FAILURE;
    } catch (FijiInvalidNameException kine) {
      getPrintStream().println("Invalid Fiji instance: " + kine.getMessage());
      return FAILURE;
    } catch (FijiNotInstalledException knie) {
      getPrintStream().printf("Fiji instance '%s' is not installed.%n", mFijiURI);
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
    System.exit(new FijiToolLauncher().run(new UninstallTool(), args));
  }
}
