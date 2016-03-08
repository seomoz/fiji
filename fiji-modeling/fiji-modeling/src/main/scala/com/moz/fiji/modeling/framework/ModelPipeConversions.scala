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

package com.moz.fiji.modeling.framework

import cascading.pipe.Pipe
import cascading.flow.FlowDef
import com.twitter.scalding.Mode

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.annotations.Inheritance
import com.moz.fiji.express.flow.FijiSource
import com.moz.fiji.modeling.lib.RecommendationPipe

/**
 * ModelPipeConversions contains implicit conversions necessary for FijiModeling that are not
 * included in FijiExpress's FijiJob.
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
trait ModelPipeConversions {

  /**
   * Converts a Cascading Pipe to an Express Recommendation Pipe. This method permits implicit
   * conversions from Pipe to RecommendationPipe.
   *
   * @param pipe to convert to a RecommendationPipe.
   * @return a RecommendationPipe wrapping the specified Pipe.
   */
  implicit def pipe2RecommendationPipe(pipe: Pipe): RecommendationPipe =
    new RecommendationPipe(pipe)

  /**
   * Converts a FijiSource to a FijiExpress Recommendation Pipe. This method permits implicit
   * conversions from Source to RecommendationPipe.
   *
   * We expect flowDef and mode implicits to be in scope.  This should be true in the context of a
   * Job, FijiJob, or inside the ShellRunner.
   *
   * @param source to convert to a FijiPipe.
   * @return a RecommendationPipe read from the specified source.
   */
  implicit def source2RecommendationPipe(
      source: FijiSource)(
      implicit flowDef: FlowDef,
      mode: Mode): RecommendationPipe = new RecommendationPipe(source.read(flowDef, mode))
}
