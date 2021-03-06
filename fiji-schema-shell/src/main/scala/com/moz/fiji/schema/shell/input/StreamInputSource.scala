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

package com.moz.fiji.schema.shell.input

import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader

import com.moz.fiji.annotations.ApiAudience

/**
 * Reads user input from an InputStream full of commands.
 *
 * <p>The close() method of this InputSource will close the underlying stream.</p>
 */
@ApiAudience.Private
final class StreamInputSource(stream: InputStream) extends InputSource {
  private val bufferedStream = new BufferedReader(new InputStreamReader(stream, "UTF-8"))

  /** Read a line of input from the underlying source and return it. */
  def readLine(prompt: String): Option[String] = {
    return Option(bufferedStream.readLine())
  }

  def close(): Unit = {
    bufferedStream.close()
  }
}
