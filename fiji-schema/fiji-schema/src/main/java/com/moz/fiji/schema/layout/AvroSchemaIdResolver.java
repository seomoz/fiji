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

package com.moz.fiji.schema.layout;

import java.io.IOException;

import com.google.common.base.Function;
import org.apache.avro.Schema;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.FijiIOException;
import com.moz.fiji.schema.FijiSchemaTable;
import com.moz.fiji.schema.avro.AvroSchema;

/**
 * Resolves AvroSchema descriptors into Schema IDs.
 *
 * <p> This resolver potentially allocates schema IDs in the schema table. </p>
 */
@ApiAudience.Private
public final class AvroSchemaIdResolver implements Function<AvroSchema, Long> {
  /** Schema table used to resolve Avro schemas. */
  private final FijiSchemaTable mSchemaTable;

  /**
   * Creates a new Avro schema resolver.
   *
   * @param schemaTable Resolve schema UIDs against this schema table.
   */
  public AvroSchemaIdResolver(final FijiSchemaTable schemaTable) {
    mSchemaTable = schemaTable;
  }

  /** {@inheritDoc} */
  @Override
  public Long apply(AvroSchema avroSchema) {
    if (avroSchema.getJson() != null) {
      final Schema schema = new Schema.Parser().parse(avroSchema.getJson());
      try {
        return mSchemaTable.getOrCreateSchemaId(schema);
      } catch (IOException ioe) {
        throw new FijiIOException(ioe);
      }
    } else if (avroSchema.getUid() != null) {
      return avroSchema.getUid();
    } else {
      throw new FijiIOException(
          "AvroSchema neither has a schema UID nor a schema JSON descriptor.");
    }
  }
}
