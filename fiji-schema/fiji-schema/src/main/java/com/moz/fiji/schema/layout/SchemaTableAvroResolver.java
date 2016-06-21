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

import org.apache.avro.Schema;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.FijiIOException;
import com.moz.fiji.schema.FijiSchemaTable;
import com.moz.fiji.schema.avro.AvroSchema;

/** Resolves AvroSchema descriptors into Schema objects through a {@link FijiSchemaTable}. */
@ApiAudience.Private
public final class SchemaTableAvroResolver implements AvroSchemaResolver {
  /** Schema table used to resolve Avro schemas. */
  private final FijiSchemaTable mSchemaTable;

  /**
   * Creates a new Avro schema resolver.
   *
   * @param schemaTable Resolve schema UIDs against this schema table.
   */
  public SchemaTableAvroResolver(final FijiSchemaTable schemaTable) {
    mSchemaTable = schemaTable;
  }

  /** {@inheritDoc} */
  @Override
  public Schema apply(AvroSchema avroSchema) {
    if (avroSchema.getJson() != null) {
      return new Schema.Parser().parse(avroSchema.getJson());
    } else if (avroSchema.getUid() != null) {
      try {
        final Schema schema = mSchemaTable.getSchema(avroSchema.getUid());
        if (schema == null) {
          throw new FijiIOException(String.format(
              "Schema UID %d unknown in Fiji instance '%s'.",
              avroSchema.getUid(), mSchemaTable));
        }
        return schema;
      } catch (IOException ioe) {
        throw new FijiIOException(ioe);
      }
    } else {
      throw new FijiIOException(
          "AvroSchema neither has a schema UID nor a schema JSON descriptor.");
    }
  }
}
