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
package com.moz.fiji.schema.impl;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiEncodingException;
import com.moz.fiji.schema.avro.AvroValidationPolicy;
import com.moz.fiji.schema.avro.CellSchema;
import com.moz.fiji.schema.avro.SchemaType;
import com.moz.fiji.schema.layout.CellSpec;

public class TestAvroCellEncoder extends FijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestAvroCellEncoder.class);

  /**
   * Tests that AvroCellEncoder throws an appropriate FijiEncodingException if no Avro writer
   * schema can be inferred for the specified value to encode.
   */
  @Test
  public void testWriterSchemaNotInferrable() throws IOException {
    final Fiji fiji = getFiji();
    final CellSpec cellSpec = CellSpec.create()
        .setCellSchema(CellSchema.newBuilder()
            .setType(SchemaType.AVRO)
            .setAvroValidationPolicy(AvroValidationPolicy.DEVELOPER)
            .build())
        .setSchemaTable(fiji.getSchemaTable());
    final AvroCellEncoder encoder = new AvroCellEncoder(cellSpec);
    try {
      encoder.encode(new Object());
      Assert.fail("AvroCellEncoder.encode() should throw a FijiEncodingException.");
    } catch (FijiEncodingException kee) {
      LOG.info("Expected error: '{}'", kee.getMessage());
      Assert.assertTrue(kee.getMessage().contains("Unable to infer Avro writer schema for value"));
    }
  }

}
