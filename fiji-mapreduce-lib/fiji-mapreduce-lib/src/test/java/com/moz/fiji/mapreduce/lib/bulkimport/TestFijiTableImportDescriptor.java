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

package com.moz.fiji.mapreduce.lib.bulkimport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.Test;

import com.moz.fiji.mapreduce.FijiMRTestLayouts;
import com.moz.fiji.mapreduce.TestingResources;
import com.moz.fiji.mapreduce.lib.avro.TableImportDescriptorDesc;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.util.FromJson;

/** Unit tests. */
public class TestFijiTableImportDescriptor {

  @Test
  public void testFoo() throws IOException {
    final String json = TestingResources.get(BulkImporterTestUtils.FOO_IMPORT_DESCRIPTOR);
    TableImportDescriptorDesc mappingDesc =
        (TableImportDescriptorDesc) FromJson.fromJsonString(json,
            TableImportDescriptorDesc.SCHEMA$);
    FijiTableImportDescriptor mapping = new FijiTableImportDescriptor(mappingDesc);
    assertEquals("foo", mapping.getName());
    assertEquals(4, mapping.getColumnNameSourceMap().size());
  }

  @Test
  public void testValidation() throws IOException {
    final String json = TestingResources.get(BulkImporterTestUtils.FOO_IMPORT_DESCRIPTOR);

    TableImportDescriptorDesc mappingDesc =
        (TableImportDescriptorDesc) FromJson.fromJsonString(json,
            TableImportDescriptorDesc.SCHEMA$);

    FijiTableImportDescriptor mapping = new FijiTableImportDescriptor(mappingDesc);
    final FijiTableLayout fooLayout =
        FijiTableLayout.newLayout(FijiMRTestLayouts.getTestLayout());
    mapping.validateDestination(fooLayout);
  }

  @Test
  public void testValidationFail() throws IOException {
    final String json = TestingResources.get(BulkImporterTestUtils.FOO_INVALID_DESCRIPTOR);

    TableImportDescriptorDesc mappingDesc =
        (TableImportDescriptorDesc) FromJson.fromJsonString(json,
            TableImportDescriptorDesc.SCHEMA$);

    FijiTableImportDescriptor mapping = new FijiTableImportDescriptor(mappingDesc);
    final FijiTableLayout fooLayout =
        FijiTableLayout.newLayout(FijiMRTestLayouts.getTestLayout());
    try {
      mapping.validateDestination(fooLayout);
      fail("Should've gotten an InvalidTableImportDescription by here.");
    } catch (InvalidTableImportDescriptorException ie) {
      assertEquals("Table 'test' does not contain column 'info:first_name_nonexistant'.",
          ie.getMessage());
    }
  }
}
