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

package com.moz.fiji.schema.shell.ddl

import scala.math._

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.schema.shell.Environment

/** List available Fiji instances. */
@ApiAudience.Private
final class ShowInstancesCommand(val env: Environment) extends DDLCommand with StrFormatting {
  override def exec(): Environment = {
    val instances = env.fijiSystem.listInstances()

    if (instances.size == 0) {
      // No instances to list.
      echo("(No instances available)")
      return env
    }

    // Get the max instance name length for use in printing.
    // Add two characters for the current instance '*'
    val maxNameLength = max("instance".length,
        instances.maxBy{ name => name.length() }.length())

    // Build an output, starting with a header.
    val output = new StringBuilder()
    output.append("  Instance:\n")
        .append("=" * (maxNameLength + 2))
        .append("\n")
    // Add the instance names
    instances.foreach { case name =>
      if (name.equals(getFijiURI().getInstance())) {
        output.append("* ") // Currently-selected instance.
      } else {
        output.append("  ")
      }
      output.append(padTo(name, maxNameLength))
          .append("\n")
    }
    echoNoNL(output.toString())
    return env
  }
}
