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

package com.moz.fiji.schema.layout.impl;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.codehaus.jackson.node.IntNode;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.avro.AvroValidationPolicy;
import com.moz.fiji.schema.avro.TableLayoutDesc;
import com.moz.fiji.schema.avro.TestRecord4;
import com.moz.fiji.schema.avro.TestRecord5;
import com.moz.fiji.schema.impl.Versions;
import com.moz.fiji.schema.layout.InvalidLayoutException;
import com.moz.fiji.schema.layout.InvalidLayoutSchemaException;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.layout.TableLayoutBuilder;

/** Tests for TableLayoutUpdateValidator. */
public class TestTableLayoutUpdateValidator extends FijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestTableLayoutUpdateValidator.class);

  /**
   * Layout for table 'invalid_avro_validation_test' to test Avro validation in pre-layout-1.3 mode.
   * Layout is internally incompatible, but uses layout-1.2.0 and so is not validated.
   */
  public static final String INVALID_AVRO_VALIDATION_TEST =
      "com.moz.fiji/schema/layout/invalid-avro-validation-test.json";

  /**
   * layout-1.3.0 based Layout for table 'avro_validation_test' which includes a valid set of reader
   * and writer schemas.
   */
  public static final String AVRO_VALIDATION_TEST =
      "com.moz.fiji/schema/layout/avro-validation-test.json";

  /**
   * layout-1.3.0 based Layout for table 'avrovalidationtest' which includes added, modified, and
   * removed columns from {@link #AVRO_VALIDATION_TEST}.
   *
   * <pre>
   *   Updates:
   *     info:qual0 modified to include a new reader with an additional optional field
   *     info:qual1 removed
   *     info:qual2 added
   * </pre>
   */
  public static final String AVRO_VALIDATION_UPDATE_TEST =
      "com.moz.fiji/schema/layout/avro-validation-update-test.json";

  /** Schema containing a single String field. */
  private static final Schema STRING_SCHEMA = Schema.create(Type.STRING);
  /**
   * Schema containing a single Int field. This will not be readable by the registered String
   * schema from the previous layout because Int and String are incompatible types.
   */
  private static final Schema INT_SCHEMA = Schema.create(Type.INT);

  /**
   * Creates a table with a string reader schema and the given validation policy
   * then builds a new TableLayoutDesc with an int writer schema.
   *
   * @param fiji the Fiji instance in which to create the table.
   * @param policy the AvroValidationPolicy to enforce on the test column.
   * @return a new TableLayoutDesc with with its reference layout set to the old string-reader
   *     layout and a new int-writer schema.
   * @throws IOException
   */
  private TableLayoutDesc prepareNewDesc(Fiji fiji, AvroValidationPolicy policy)
      throws IOException {

    final TableLayoutDesc desc = FijiTableLayouts.getLayout(FijiTableLayouts.SCHEMA_REG_TEST);
    desc.setVersion("layout-1.3.0");

    final TableLayoutDesc originalDesc = new TableLayoutBuilder(desc, fiji)
        .withAvroValidationPolicy(
            FijiColumnName.create("info:fullname"), policy)
        .withReader(FijiColumnName.create("info:fullname"), STRING_SCHEMA)
        .build();

    fiji.createTable(originalDesc);

    final TableLayoutDesc newDesc = new TableLayoutBuilder(originalDesc, fiji)
        .withWriter(FijiColumnName.create("info:fullname"), INT_SCHEMA)
        .build();
    return newDesc;
  }

  @Test
  public void testAvroValidationChanges() throws IOException {
    final TableLayoutUpdateValidator validator = new TableLayoutUpdateValidator(getFiji());
    final TableLayoutDesc basicDesc = FijiTableLayouts.getLayout(FijiTableLayouts.SCHEMA_REG_TEST);
    basicDesc.setVersion("layout-1.3.0");
    final FijiColumnName validatedColumn = FijiColumnName.create("info:fullname");
    final Schema intSchema = Schema.create(Type.INT);
    final Schema stringSchema = Schema.create(Type.STRING);

    final TableLayoutDesc strictIntDesc = new TableLayoutBuilder(basicDesc, getFiji())
        .withAvroValidationPolicy(validatedColumn, AvroValidationPolicy.STRICT)
        .withReader(validatedColumn, intSchema)
        .build();
    final TableLayoutDesc strictStringDesc = new TableLayoutBuilder(basicDesc, getFiji())
        .withAvroValidationPolicy(validatedColumn, AvroValidationPolicy.STRICT)
        .withWriter(validatedColumn, stringSchema)
        .build();
    final TableLayoutDesc developerIntDesc = new TableLayoutBuilder(basicDesc, getFiji())
        .withAvroValidationPolicy(validatedColumn, AvroValidationPolicy.DEVELOPER)
        .withReader(validatedColumn, intSchema)
        .build();
    final TableLayoutDesc developerStringDesc = new TableLayoutBuilder(basicDesc, getFiji())
        .withAvroValidationPolicy(validatedColumn, AvroValidationPolicy.DEVELOPER)
        .withWriter(validatedColumn, stringSchema)
        .build();
    final TableLayoutDesc noneIntDesc = new TableLayoutBuilder(basicDesc, getFiji())
        .withAvroValidationPolicy(validatedColumn, AvroValidationPolicy.NONE)
        .withReader(validatedColumn, intSchema)
        .build();
    final TableLayoutDesc noneStringDesc = new TableLayoutBuilder(basicDesc, getFiji())
        .withAvroValidationPolicy(validatedColumn, AvroValidationPolicy.NONE)
        .withWriter(validatedColumn, stringSchema)
        .build();
    final TableLayoutDesc schema10IntDesc = new TableLayoutBuilder(basicDesc, getFiji())
        .withAvroValidationPolicy(validatedColumn, AvroValidationPolicy.SCHEMA_1_0)
        .withReader(validatedColumn, intSchema)
        .build();
    final TableLayoutDesc schema10StringDesc = new TableLayoutBuilder(basicDesc, getFiji())
        .withAvroValidationPolicy(validatedColumn, AvroValidationPolicy.SCHEMA_1_0)
        .withWriter(validatedColumn, stringSchema)
        .build();

    final List<TableLayoutDesc> intDescs =
        Lists.newArrayList(strictIntDesc, developerIntDesc, noneIntDesc, schema10IntDesc);

    // Increasing or decreasing validation strictness is acceptable in either direction.  The new
    // layout sets the validation policy which will be run.  For an increase in validation to
    // succeed old readers and writers must be internally compatible and compatible with new readers
    // and writers.

    // Test each validation mode modified to STRICT.
    for (TableLayoutDesc intDesc : intDescs) {
      // Increasing the layout validation to STRICT should throw an exception.
      try {
        validator.validate(
            FijiTableLayout.newLayout(intDesc), FijiTableLayout.newLayout(strictStringDesc));
        fail("validate should have thrown InvalidLayoutSchemaException because int and string are "
            + "incompatible.");
      } catch (InvalidLayoutSchemaException ilse) {
        assertTrue(ilse.getReasons().contains(
            "In column: 'info:fullname' Reader schema: \"int\" is incompatible with "
            + "writer schema: \"string\"."));
        assertTrue(ilse.getReasons().size() == 1);
      }
    }

    // Test each validation mode modified to DEVELOPER.
    for (TableLayoutDesc intDesc : intDescs) {
      // Increasing the layout validation to DEVELOPER should throw an exception.
      try {
        validator.validate(
            FijiTableLayout.newLayout(intDesc), FijiTableLayout.newLayout(developerStringDesc));
        fail("validate should have thrown InvalidLayoutSchemaException because int and string are "
            + "incompatible.");
      } catch (InvalidLayoutSchemaException ilse) {
        assertTrue(ilse.getReasons().contains(
            "In column: 'info:fullname' Reader schema: \"int\" is incompatible with "
            + "writer schema: \"string\"."));
        assertTrue(ilse.getReasons().size() == 1);
      }
    }

    // Test each validation mode modified to SCHEMA_1_0.
    for (TableLayoutDesc intDesc : intDescs) {
      // Reducing the layout validation to SCHEMA_1_0 should eliminate errors.
      validator.validate(
          FijiTableLayout.newLayout(intDesc), FijiTableLayout.newLayout(schema10StringDesc));
    }

    // Test each validation mode modified to NONE.
    for (TableLayoutDesc intDesc : intDescs) {
      // Reducing the layout validation to NONE should eliminate errors.
      validator.validate(
            FijiTableLayout.newLayout(intDesc), FijiTableLayout.newLayout(noneStringDesc));
    }
  }

  @Test
  public void testOldLayoutVersions() throws IOException {
    final TableLayoutUpdateValidator validator = new TableLayoutUpdateValidator(getFiji());
    {
      final TableLayoutDesc desc =
          FijiTableLayouts.getLayout(INVALID_AVRO_VALIDATION_TEST);
      desc.setVersion(Versions.LAYOUT_1_2_0.toString());

      try {
        FijiTableLayout.createUpdatedLayout(desc,  null);
        fail("Must throw InvalidLayoutException "
            + "because AVRO cell type requires layout version >= 1.3.0");
      } catch (InvalidLayoutException ile) {
        LOG.info("Expected error: {}", ile.getMessage());
        assertTrue(ile.getMessage(),
            ile.getMessage().contains(
                "Cell type AVRO requires table layout version >= layout-1.3.0, "
                + "got version layout-1.2.0"));
      }
    }

    // Layout-1.3.0 does support validation.  The layout is invalid and will throw an exception.
    final TableLayoutDesc desc =
        FijiTableLayouts.getLayout(INVALID_AVRO_VALIDATION_TEST);
    try {
      validator.validate(null, FijiTableLayout.newLayout(desc));
      fail("should have thrown InvalidLayoutSchemaException because int and string are "
          + "incompatible.");
    } catch (InvalidLayoutSchemaException ilse) {
      assertTrue(ilse.getReasons().contains(
          "In column: 'info:fullname' Reader schema: \"int\" is incompatible with "
          + "writer schema: \"string\"."));
    }
  }

  @Test
  public void testAddRemoveModifyColumns() throws IOException {
    final TableLayoutUpdateValidator validator = new TableLayoutUpdateValidator(getFiji());
    final FijiColumnName validatedColumn = FijiColumnName.create("info:qual0");
    final TableLayoutDesc desc = new TableLayoutBuilder(
        FijiTableLayouts.getLayout(AVRO_VALIDATION_TEST), getFiji())
        .withReader(validatedColumn, TestRecord5.SCHEMA$)
        .withAvroValidationPolicy(validatedColumn, AvroValidationPolicy.STRICT)
        .withLayoutId("original")
        .build();

    // The initial layout is valid.
    validator.validate(null, FijiTableLayout.newLayout(desc));

    // Create an update which removes a column, adds a column, and modifies the set of readers for a
    // column in a compatible way.
    final TableLayoutDesc updateDesc =
        FijiTableLayouts.getLayout(AVRO_VALIDATION_UPDATE_TEST);
    updateDesc.setReferenceLayout("original");
    updateDesc.setLayoutId("updated");
    final TableLayoutDesc newDesc = new TableLayoutBuilder(updateDesc, getFiji())
        .withReader(validatedColumn, TestRecord4.SCHEMA$)
        .build();

    validator.validate(FijiTableLayout.newLayout(desc), FijiTableLayout.newLayout(newDesc));
  }

  /**
   * Tests the layout update validator on the following scenario:
   *  - initial layout has a reader for an empty record
   *  - layout update adds a writer schema with a compatible record with one optional integer.
   */
  @Test
  public void testValidLayoutUpdate() throws IOException {
    final Fiji fiji = getFiji();

    final TableLayoutDesc desc = FijiTableLayouts.getLayout(FijiTableLayouts.SCHEMA_REG_TEST);
    desc.setVersion("layout-1.3.0");

    final Schema emptyRecordSchema = Schema.createRecord("Record", null, "org.ns", false);
    emptyRecordSchema.setFields(Lists.<Field>newArrayList());

    final Schema optIntRecordSchema = Schema.createRecord("Record", null, "org.ns", false);
    optIntRecordSchema.setFields(Lists.newArrayList(
        new Field("a", INT_SCHEMA, null, IntNode.valueOf(0))));

    final TableLayoutDesc originalDesc = new TableLayoutBuilder(desc, fiji)
        .withAvroValidationPolicy(
            FijiColumnName.create("info:fullname"), AvroValidationPolicy.STRICT)
        .withReader(FijiColumnName.create("info:fullname"), emptyRecordSchema)
        .build();

    fiji.createTable(originalDesc);

    final TableLayoutDesc newDesc = new TableLayoutBuilder(originalDesc, fiji)
        .withWriter(FijiColumnName.create("info:fullname"), optIntRecordSchema)
        .build();

    fiji.modifyTableLayout(newDesc);
  }

  @Test
  public void testStrictValidation() throws IOException {
    final Fiji fiji = getFiji();

    // Strict validation should fail.
    final TableLayoutDesc strictDesc = prepareNewDesc(fiji, AvroValidationPolicy.STRICT);
    try {
      fiji.modifyTableLayout(strictDesc);
      fail("Should have thrown an InvalidLayoutSchemaException because int and string are "
          + "incompatible.");
    } catch (InvalidLayoutSchemaException ilse) {
      assertTrue(ilse.getReasons().contains(
          "In column: 'info:fullname' Reader schema: \"string\" is incompatible with "
              + "writer schema: \"int\"."));
      assertTrue(ilse.getReasons().size() == 1);
    }
  }

  @Test
  public void testDeveloperValidation() throws IOException {
    final Fiji fiji = getFiji();

    // Developer validation should fail.
    final TableLayoutDesc developerDesc = prepareNewDesc(fiji, AvroValidationPolicy.DEVELOPER);
    try {
      fiji.modifyTableLayout(developerDesc);
      fail("Should have thrown an InvalidLayoutSchemaException because int and string are "
          + "incompatible.");
    } catch (InvalidLayoutSchemaException ilse) {
      assertTrue(ilse.getReasons().contains(
          "In column: 'info:fullname' Reader schema: \"string\" is incompatible with "
              + "writer schema: \"int\"."));
      assertTrue(ilse.getReasons().size() == 1);
    }
  }

  @Test
  public void testNoneValidation() throws IOException {
    final Fiji fiji = getFiji();

    // None validation should pass despite the incompatible change.
    final TableLayoutDesc noneDesc = prepareNewDesc(fiji, AvroValidationPolicy.NONE);
    fiji.modifyTableLayout(noneDesc);
  }

  @Test
  public void testSchema10Validation() throws IOException {
    final Fiji fiji = getFiji();

    // Schema-1.0 validation should pass despite incompatible changes.
    final TableLayoutDesc schema10Desc = prepareNewDesc(fiji, AvroValidationPolicy.SCHEMA_1_0);
    fiji.modifyTableLayout(schema10Desc);
  }
}
