/**
 * (c) Copyright 2014 WibiData, Inc.
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

package com.moz.fiji.express.flow.framework.serialization

import com.esotericsoftware.kryo.DefaultSerializer
import com.esotericsoftware.kryo.serializers.JavaSerializer
import com.twitter.scalding.serialization.Externalizer
import com.twitter.chill.config.ScalaMapConfig

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.annotations.Inheritance

/**
 * Constructors for wrapping objects with a java-serializable container using Kryo.
 */
@ApiAudience.Private
@ApiStability.Stable
object FijiKryoExternalizer {
  def apply[T](t: T): FijiKryoExternalizer[T] = {
    val externalizer = new FijiKryoExternalizer[T]
    externalizer.set(t)
    externalizer
  }
}

/**
 * Serializable container for wrapping objects using Kryo.
 *
 * To use this:
 * {{{
 *   val myNonSerializableThings = //...
 *
 *   // Wrap your data in a serializable container.
 *   val myNowSerializableThings = FijiKryoExternalizer(myNonSerializableThings)
 *
 *   // To reconstitute your original data:
 *   val myOriginalThings = myNowSerializableThings.get
 *
 *   // - or -
 *
 *   val myOriginalThings = myNowSerializableThings.getOption
 * }}}
 */
@ApiAudience.Private
@ApiStability.Stable
@Inheritance.Sealed
@DefaultSerializer(classOf[JavaSerializer])
class FijiKryoExternalizer[T] extends Externalizer[T] {
  protected override def kryo =
      new FijiKryoInstantiator(ScalaMapConfig(Map("scalding.kryo.setreferences" -> "true")))
}
