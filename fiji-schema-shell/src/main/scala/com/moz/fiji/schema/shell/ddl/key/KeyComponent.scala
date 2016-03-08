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

package com.moz.fiji.schema.shell.ddl.key

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.schema.avro.RowKeyComponent
import com.moz.fiji.schema.shell.ddl.key.RowKeyElemType._

/**
 * A parameter to a FormattedKeySpec specifying one named component
 * in the key.
 *
 * @param name the name of the key component.
 * @param typ the type of the component.
 * @param mayBeNull is true if the key component is nullable.
 */
@ApiAudience.Private
final class KeyComponent(val name: String, val typ: RowKeyElemType, val mayBeNull: Boolean)
    extends FormattedKeyParam {

  /**
   * @return an Avro RowKeyComponent representing this key component.
   */
  def toAvro(): RowKeyComponent = {
    return RowKeyComponent.newBuilder()
        .setName(name)
        .setType(RowKeyElemType.toComponentType(typ))
        .build()
  }
}
