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
import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configured;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.common.flags.Flag;
import com.moz.fiji.common.flags.FlagParser;
import com.moz.fiji.delegation.Lookups;
import com.moz.fiji.schema.util.ResourceUtils;
import com.moz.fiji.schema.util.Resources;

/**
 * Command-line tool for displaying help on available tools.
 */
@ApiAudience.Private
public final class HelpTool extends Configured implements FijiTool {

  /** Maximum padding width for the name column in the help display. */
  private static final int MAX_NAME_WIDTH = 24;

  @Flag(name="verbose", usage="Enable verbose help")
  private boolean mVerbose = false;

  @Flag(name="help", usage="Print the usage message.")
  private boolean mHelp = false;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "help";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Describe available Fiji tools.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Help";
  }

  /** {@inheritDoc} */
  @Override
  public String getUsageString() {
    return
        "Usage:'n"
        + "    fiji help\n";
  }

  /** Prints the tool usage message. */
  private void printUsage() {
    System.out.println(getUsageString());
    System.out.println("Flags:");
    FlagParser.printUsage(this, System.out);
  }

  /** {@inheritDoc} */
  @Override
  public int toolMain(List<String> args) throws Exception {
    List<String> nonFlagArgs = FlagParser.init(this, args.toArray(new String[args.size()]));
    if (null == nonFlagArgs) {
      // There was a problem parsing the flags.
      return BaseTool.FAILURE;
    }

    if (mHelp) {
      printUsage();
      return BaseTool.SUCCESS;
    }

    if (nonFlagArgs.size() > 0) {
      String toolName = nonFlagArgs.get(0);
      FijiTool subTool = new FijiToolLauncher().getToolForName(toolName);
      if (null != subTool) {
        System.out.println(subTool.getName() + ": " + subTool.getDescription());
        System.out.println("");
        subTool.toolMain(Collections.singletonList("--help"));
        return 0;
      } else {
        System.out.println("Error - no such tool: " + toolName);
        System.out.println("Type 'fiji help' to see all available tools.");
        System.out.println("Type 'fiji help --verbose' for additional information.");
        System.out.println("Type 'fiji help <toolName>' for tool-specific help.");
        System.out.println("");
        return 0;
      }
    }

    System.out.println("The fiji script runs tools for interacting with the Fiji system.");
    System.out.println("");
    System.out.println("USAGE");
    System.out.println("");
    System.out.println("  fiji <tool> [FLAGS]...");
    System.out.println("");
    System.out.println("TOOLS");
    System.out.println("");

    for (FijiTool tool : Lookups.get(FijiTool.class)) {
      String name = tool.getName();
      if (null == name) {
        System.out.println("Error: Got null from getName() in class: "
            + tool.getClass().getName());
        continue;
      }

      String desc = tool.getDescription();
      if (null != desc) {
        System.out.print("  " + name);
        int padding = MAX_NAME_WIDTH - name.length();
        for (int i = 0; i < padding; i++) {
          System.out.print(" ");
        }
        System.out.print(desc);
      }
      System.out.println("");
    }

    System.out.println("");
    System.out.println("  classpath               Print the classpath used to run fiji tools.");
    System.out.println("  jar                     Run a class from a user-supplied jar file.");
    System.out.println("");
    System.out.println("FLAGS");
    System.out.println("");
    System.out.println("  The available flags depend on which tool you use.  To see");
    System.out.println("  flags for a tool, use --help.  For example:");
    System.out.println("");
    System.out.println("  $ fiji <tool> --help");
    if (mVerbose) {
      printVerboseHelp();
    } else {
      System.out.println("");
      System.out.println("  To add additional jars to the classpath when running a tool,");
      System.out.println("  specify them in the FIJI_CLASSPATH environmental variable.");
      System.out.println("  For more about Fiji environment variables, "
          + "type 'fiji help --verbose'.");
    }
    return 0;
  }

  /**
   * Print details of environment variables and so-on.
   * @throws IOException on I/O error.
   */
  private static void printVerboseHelp() throws IOException {
    final InputStream envHelp = Preconditions.checkNotNull(
        Resources.openSystemResource("com.moz.fiji/schema/tools/HelpTool.envHelp.txt"));
    try {
      IOUtils.copy(envHelp, System.out);
    } finally {
      ResourceUtils.closeOrLog(envHelp);
    }
  }
}
