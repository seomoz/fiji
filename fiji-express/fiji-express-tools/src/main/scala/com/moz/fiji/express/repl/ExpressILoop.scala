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

package com.moz.fiji.express.repl

import scala.tools.nsc.interpreter.ILoop

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.annotations.Inheritance
import com.moz.fiji.schema.shell.ShellMain

/**
 * A class providing FijiExpress specific commands for inclusion in the FijiExpress REPL.
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
private[express] class ExpressILoop extends ILoop {
  /**
   * Commands specific to the FijiExpress REPL. To define a new command use one of the following
   * factory methods:
   * - `LoopCommand.nullary` for commands that take no arguments
   * - `LoopCommand.cmd` for commands that take one string argument
   * - `LoopCommand.varargs` for commands that take multiple string arguments
   */
  private val expressCommands: List[LoopCommand] = List(
      LoopCommand.varargs("schema-shell",
          "",
          "Runs the FijiSchema shell.",
          schemaShellCommand)
  )

  /**
   * Change the shell prompt to read express&gt;
   *
   * @return a prompt string to use for this REPL.
   */
  override def prompt: String = "\nexpress> "

  /**
   * Gets the list of commands that this REPL supports.
   *
   * @return a list of the command supported by this REPL.
   */
  override def commands: List[LoopCommand] = super.commands ++ expressCommands

  /**
   * Determines whether the fiji-schema-shell jar is on the classpath.
   *
   * @return `true` if fiji-schema-shell is on the classpath, `false` otherwise.
   */
  private def isSchemaShellEnabled: Boolean = {
    try {
      ShellMain.version()
      true
    } catch {
      case _: NoClassDefFoundError => false
    }
  }

  /**
   * Runs an instance of the FijiSchema Shell within the Scala REPL.
   *
   * @param args that should be passed to the instance of FijiSchema Shell to run.
   * @return the result of running the command, which should always be the default result.
   */
  private def schemaShellCommand(args: List[String]): Result = {
    if (isSchemaShellEnabled) {
      try {
        // Create a shell runner and use it to run an instance of the REPL with no arguments.
        val shellMain = new ShellMain()
        // Run the shell.
        val exitCode = shellMain.run()
        if (exitCode == 0) {
          Result.resultFromString("FijiSchema Shell exited with success.")
        } else {
          Result.resultFromString("FijiSchema Shell exited with code: " + exitCode)
        }
      } finally {
        // Close all connections properly before exiting.
        ShellMain.shellFijiSystem.shutdown()
      }
    } else {
      Result.resultFromString("The FijiSchema Shell jar is not on the classpath. "
          + "Set the environment variable SCHEMA_SHELL_HOME to the root of a FijiSchema Shell "
          + "distribution before running the FijiExpress Shell to enable FijiSchema Shell "
          + "features.")
    }
  }
}
