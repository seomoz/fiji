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

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.annotations.Inheritance
import com.moz.fiji.express.flow.framework.ExpressConversions
import com.moz.fiji.express.flow.util.PipeConversions

import cascading.pipe.Pipe

/**
 * Implicits used to construct pipes within the REPL.
 */
@ApiAudience.Framework
@ApiStability.Experimental
object ReplImplicits {

  /**
   * Converts a Pipe to a FijiPipeTool. This method permits implicit conversions from Pipe to
   * FijiPipeTool.
   *
   * @param pipe to convert to a FijiPipeTool.
   * @return a FijiPipeTool created from the specified Pipe.
   */
  implicit def pipeToFijiPipeTool(pipe: Pipe): FijiPipeTool = new FijiPipeTool(pipe)

  /**
   * Converts a FijiPipeTool to a Pipe. This method permits implicit conversions from
   * FijiPipeTool to Pipe.
   *
   * @param fijiPipeTool to convert to a Pipe.
   * @return the Pipe wrapped by the specified FijiPipeTool.
   */
  implicit def fijiPipeToolToPipe(fijiPipeTool: FijiPipeTool): Pipe = fijiPipeTool.pipe
}
