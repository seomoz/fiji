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

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.schema.shell.Environment
import com.moz.fiji.schema.shell.InputProcessor
import com.moz.fiji.schema.shell.input.FileInputSource
import com.moz.fiji.schema.util.ResourceUtils

/**
  * Run the commands in the specified file.
  * This does not change the user's active environment. It runs in a nested environment.
  */
@ApiAudience.Private
final class LoadFileCommand(val env: Environment, val filename: String) extends DDLCommand {
  override def exec(): Environment = {
    val inputSource = new FileInputSource(filename)
    try {
      new InputProcessor().processUserInput(new StringBuilder(), env.withInputSource(inputSource))
    } finally {
      ResourceUtils.closeOrLog(inputSource)
    }
    return env // Restore the original environment after processing the file.
  }
}
