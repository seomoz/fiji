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
import com.moz.fiji.schema.avro.BloomType
import com.moz.fiji.schema.avro.CompressionType
import com.moz.fiji.schema.avro.FamilyDesc
import com.moz.fiji.schema.avro.LocalityGroupDesc

/** Defines how to initialize a new LocalityGroup definition. */
@ApiAudience.Private
trait NewLocalityGroup {
  /** @return a new LocalityGroupDesc builder with empty family lists. */
  def newLocalityGroup(): LocalityGroupDesc.Builder = {
    val locGroup = LocalityGroupDesc.newBuilder()
    locGroup.setAliases(new java.util.ArrayList[String]())
    locGroup.setFamilies(new java.util.ArrayList[FamilyDesc]())
    locGroup.setDescription("")
    locGroup.setEnabled(true)
    locGroup.setCompressionType(CompressionType.NONE)
    locGroup.setMaxVersions(Int.MaxValue)
    locGroup.setTtlSeconds(Int.MaxValue)
    locGroup.setInMemory(false)
    locGroup.setBloomType(BloomType.NONE)
    return locGroup
  }
}
