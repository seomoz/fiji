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

package com.moz.fiji.express.flow.util

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import com.twitter.scalding.Mode
import com.twitter.scalding.RichPipe
import com.twitter.scalding.TypedPipe

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.annotations.Inheritance
import com.moz.fiji.express.flow.ExpressResult
import com.moz.fiji.express.flow.FijiPipe
import com.moz.fiji.express.flow.FijiSource
import com.moz.fiji.express.flow.TypedFijiSource

/**
 * PipeConversions contains implicit conversions necessary for FijiExpress that are not included in
 * Scalding's `Job`.
 */
@ApiAudience.Private
@ApiStability.Stable
@Inheritance.Sealed
private[express] trait PipeConversions {
  import PipeConversions._
  /**
   * Converts a Cascading Pipe to a FijiExpress FijiPipe. This method permits implicit conversions
   * from Pipe to FijiPipe.
   *
   * @param pipe to convert to a FijiPipe.
   * @return a FijiPipe wrapping the specified Pipe.
   */
  implicit def pipe2FijiPipe(pipe: Pipe): FijiPipe = new FijiPipe(pipe)

  /**
   * Converts a [[com.moz.fiji.express.flow.FijiPipe]] to a [[cascading.pipe.Pipe]].  This
   * method permits implicit conversion from FijiPipe to Pipe.
   *
   * @param fijiPipe to convert to [[cascading.pipe.Pipe]].
   * @return Pipe instance wrapped by the input FijiPipe.
   */
  implicit def fijiPipe2Pipe(fijiPipe: FijiPipe): Pipe = fijiPipe.pipe

  /**
   * Converts a [[com.moz.fiji.express.flow.FijiPipe]] to a [[com.twitter.scalding.RichPipe]].  This
   * method permits implicit conversion from FijiPipe to RichPipe.
   * @param fijiPipe to convert to [[com.twitter.scalding.RichPipe]].
   * @return RichPipe instance of Pipe wrapped by input FijiPipe.
   */
  implicit def fijiPipe2RichPipe(fijiPipe: FijiPipe): RichPipe = new RichPipe(fijiPipe.pipe)

  /**
   * Converts a FijiSource to a FijiExpress FijiPipe. This method permits implicit conversions
   * from Source to FijiPipe.
   *
   * We expect flowDef and mode implicits to be in scope.  This should be true in the context of a
   * Job, FijiJob, or inside the ShellRunner.
   *
   * @param source to convert to a FijiPipe
   * @return a FijiPipe read from the specified source.
   */
  implicit def source2RichPipe(
      source: FijiSource)(
      implicit flowDef: FlowDef,
      mode: Mode): FijiPipe = new FijiPipe(source.read(flowDef, mode))

  /**
   * Converts a [[TypedFijiSource]] to a TypedPipe. This method allows implicit conversion of a
   * TypedFijiSource to a TypedPipe.
   *
   * We expect flowDef and mode implicits to be in scope. This should be true in the context of a
   * Job, FijiJob, or inside the ShellRunner.
   * @param source to convert into the TypedPipe
   * @tparam T is the type parameter
   * @return
   */
  implicit def typedSource2TypedPipe[T](
      source: TypedFijiSource[T])(
      implicit flowDef: FlowDef,
      mode: Mode): TypedPipe[ExpressResult] = TypedPipe.from(source)(flowDef, mode)

  /**
   * Pimp my class to allow TypedPipe to have an .thenDo function, the same as RichPipe.
   */
  implicit def typedPipeWithThenDo[T](pipe: TypedPipe[T]): AndThen[TypedPipe[T]] = new AndThen(pipe)
}

object PipeConversions {
  /**
   * Class to add andThen and thenDo to a class.
   */
  class AndThen[T](obj: T) {
    def andThen[B](then: T => B): B = then(obj)
    def thenDo[B](then: T => B): B = then(obj)
  }
}
