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

package com.moz.fiji.schema;

import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.schema.avro.AvroSchema;
import com.moz.fiji.schema.avro.AvroValidationPolicy;
import com.moz.fiji.schema.avro.CellSchema;
import com.moz.fiji.schema.avro.Edge;
import com.moz.fiji.schema.avro.Node;
import com.moz.fiji.schema.avro.SchemaType;
import com.moz.fiji.schema.avro.TableLayoutDesc;
import com.moz.fiji.schema.layout.FijiTableLayouts;

/** Tests the schema validation on write. */
public class TestSchemaValidationOnWrite extends FijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestSchemaValidationOnWrite.class);
  private static final Schema STRING_SCHEMA = Schema.create(Schema.Type.STRING);

  /** Test write-path validation using strict Avro validation. */
  @Test
  public void testStrictWrite() throws Exception {
    final Fiji fiji = getFiji();
    final long writerUID = fiji.getSchemaTable().getOrCreateSchemaId(STRING_SCHEMA);

    // Setup table layout with pre-registered writer schemas.
    final TableLayoutDesc layoutDesc = FijiTableLayouts.getLayout(FijiTableLayouts.FULL_FEATURED);
    final CellSchema cellSchema = layoutDesc
            .getLocalityGroups().get(0)
            .getFamilies().get(0)
            .getColumns().get(0)
            .getColumnSchema();
    cellSchema.setWriters(Lists.newArrayList(AvroSchema.newBuilder().setUid(writerUID).build()));
    cellSchema.setType(SchemaType.AVRO);
    cellSchema.setAvroValidationPolicy(AvroValidationPolicy.STRICT);
    layoutDesc.setVersion("layout-1.3");
    fiji.createTable(layoutDesc);

    final FijiTable table = fiji.openTable("user");
    try {
      final FijiTableWriter writer = table.openTableWriter();
      try {
        final EntityId eid = table.getEntityId("row-key");
        final Node node = Node.newBuilder()
            .setLabel("label")
            .setWeight(1.0)
            .setEdges(null)
            .setAnnotations(null)
            .build();
        final Edge edge = Edge.newBuilder()
            .setLabel("label")
            .setWeight(1.0)
            .setTarget(node)
            .setAnnotations(null)
            .build();

        // info:name is typed as "string" and must be a CharSequence:
        writer.put(eid, "info", "name", "The user name");
        try {
          writer.put(eid, "info", "name", 1L);
          fail("Writing a long instead of a string should fail.");
        } catch (FijiEncodingException kee) {
          LOG.debug("Expected error: {}", kee.getMessage());
        }
        try {
          writer.put(eid, "info", "name", table.getLayout().getDesc());
          fail("Writing a record with an unregistered schema should fail.");
        } catch (FijiEncodingException kee) {
          LOG.debug("Expected error: {}", kee.getMessage());
        }
        try {
          writer.put(eid, "info", "name", edge);
          fail("Writing a record with an unregistered schema should fail.");
        } catch (FijiEncodingException kee) {
          LOG.debug("Expected error: {}", kee.getMessage());
        }

      } finally {
        writer.close();
      }
    } finally {
      table.release();
    }
  }
}
