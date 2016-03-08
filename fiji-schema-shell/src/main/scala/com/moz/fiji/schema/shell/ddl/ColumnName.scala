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
import com.moz.fiji.annotations.ApiStability

/**
 * Represents a parsed column name. Qualifier should never be null; map-type families
 * are represented through a different means.
 */
@ApiAudience.Framework
@ApiStability.Evolving
final class ColumnName(val family: String, val qualifier: String) {
  override def toString(): String = {
    return family + ":" + qualifier
  }

  override def hashCode(): Int = {
    return family.hashCode() ^ qualifier.hashCode()
  }

  override def equals(other: Any): Boolean = {
    other match {
      case col: ColumnName => {
        return family == col.family && qualifier == col.qualifier
      }
      case _ => { return false }
    }
  }
}
