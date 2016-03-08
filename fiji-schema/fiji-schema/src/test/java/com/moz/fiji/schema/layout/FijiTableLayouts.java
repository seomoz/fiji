/**
 * (c) Copyright 2012 WibiData, Inc.
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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;

import com.moz.fiji.schema.avro.RowKeyEncoding;
import com.moz.fiji.schema.avro.RowKeyFormat;
import com.moz.fiji.schema.avro.TableLayoutDesc;
import com.moz.fiji.schema.util.FromJson;
import com.moz.fiji.schema.util.ResourceUtils;
import com.moz.fiji.schema.util.ToJson;

/**
 * A utility class providing several table layouts for testing.
 */
public final class FijiTableLayouts {
  /**
   * Loads a table layout descriptor from a JSON resource.
   *
   * @param resourcePath Path of the resource to load the JSON descriptor from.
   * @return the decoded TableLayoutDesc.
   * @throws IOException on I/O error.
   */
  public static TableLayoutDesc getLayout(String resourcePath) throws IOException {
    final InputStream istream =
        FijiTableLayouts.class.getClassLoader().getResourceAsStream(resourcePath);
    try {
      final String json = IOUtils.toString(istream);
      return (TableLayoutDesc) FromJson.fromJsonString(json, TableLayoutDesc.SCHEMA$);
    } finally {
      ResourceUtils.closeOrLog(istream);
    }
  }

  /**
   * Creates an initial table layout from the specified JSON resource.
   *
   * @param resourcePath Path of the resource to load the JSON descriptor from.
   * @return an initial table layout constructed from the specified JSON resource.
   * @throws IOException on I/O error.
   */
  public static FijiTableLayout getTableLayout(String resourcePath) throws IOException {
    return FijiTableLayout.newLayout(getLayout(resourcePath));
  }

  /** A fully-featured layout example. */
  public static final String FULL_FEATURED = "com.moz.fiji/schema/layout/full-featured-layout.json";

  /** Fully-featured layout example with inline and class schemas. */
  public static final String FULL_FEATURED_INLINE_SCHEMA =
      "com.moz.fiji/schema/layout/full-featured-layout-inline-schemas.json";

  /**
   * A fully-featured layout example with the identity column name translator.
   */
  public static final String FULL_FEATURED_IDENTITY =
      "com.moz.fiji/schema/layout/identity-translator-layout.json";

  /**
   * A fully-featured layout example with the native column name translator.
   */
  public static final String FULL_FEATURED_NATIVE =
      "com.moz.fiji/schema/layout/native-translator-layout.json";

  /**
   * A invalid layout example with the native column name translator where the locality groups
   * don't match the column families.
   */
  public static final String INVALID_NATIVE =
      "com.moz.fiji/schema/layout/invalid-native-translator-layout.json";

  /** A simple layout for a user table. */
  public static final String USER_TABLE =
      "com.moz.fiji/schema/layout/user-table.json";

  /** A simple layout for a user table with formatted entity ids. */
  public static final String USER_TABLE_FORMATTED_EID =
      "com.moz.fiji/schema/layout/user-table-formatted-eid.json";

  /** A simple layout file example (bare minimum). */
  public static final String SIMPLE =
      "com.moz.fiji/schema/layout/simple.json";

  /** Layout of a table with no family. */
  public static final String NOFAMILY =
      "com.moz.fiji/schema/layout/nofamily.json";

  /** A simple unhashed layout file example (bare minimum). */
  public static final String SIMPLE_UNHASHED =
      "com.moz.fiji/schema/layout/simple-unhashed.json";

  /** Simple layout example with an extra int column called 'new'. */
  public static final String SIMPLE_UPDATE_NEW_COLUMN =
      "com.moz.fiji/schema/layout/simple-update-new-column.json";

  /** A simple layout file example with an extra locality group called 'new'. */
  public static final String SIMPLE_UPDATE_NEW_LOCALITY_GROUP =
      "com.moz.fiji/schema/layout/simple-update-new-locality-group.json";

  /** A simple layout file example with two columns. */
  public static final String SIMPLE_TWO_COLUMNS =
      "com.moz.fiji/schema/layout/simple-two-columns.json";

  /** A simple layout file example with two columns. */
  public static final String SIMPLE_FORMATTED_EID =
      "com.moz.fiji/schema/layout/simple-formatted-eid.json";

  /** A simple layout file example with two columns - entity ID has two components. */
  public static final String SIMPLE_FORMATTED_EID_TWO_COMPONENTS =
      "com.moz.fiji/schema/layout/simple-formatted-eid-two-components.json";

  /** A layout file which uses several primitive types as schemas for the columns. */
  public static final String PRIMITIVE_TYPES =
      "com.moz.fiji/schema/layout/primitive-types.json";

  /** A layout file with counter schemas. */
  public static final String COUNTER_TEST =
      "com.moz.fiji/schema/layout/counter-test.json";

  /** Test layout for paging. */
  public static final String PAGING_TEST =
      "com.moz.fiji/schema/layout/paging-test.json";

  public static final String ROW_DATA_TEST =
      "com.moz.fiji/schema/layout/row-data-test.json";

  /** Layout to test sqoop export map. */
  public static final String SQOOP_EXPORT_MAP_TEST =
      "com.moz.fiji/schema/layout/sqoop-export-map-test.json";

  /** Layout to test sqoop export sampling with row key hashing enabled. */
  public static final String SQOOP_EXPORT_SAMPLING_HASHED_TEST =
      "com.moz.fiji/schema/layout/sqoop-export-sampling-hashed-test.json";

  /** Layout to test sqoop export sampling with row key hashing disabled. */
  public static final String SQOOP_EXPORT_SAMPLING_UNHASHED_TEST =
      "com.moz.fiji/schema/layout/sqoop-export-sampling-unhashed-test.json";

  /** Layout to test sqoop export with varying types. */
  public static final String SQOOP_EXPORT_VARYING_TYPED_TEST =
      "com.moz.fiji/schema/layout/sqoop-export-varying-typed-test.json";

  /** Test layout named 'foo' that uses RowKeyFormat2. */
  public static final String FOO_TEST =
      "com.moz.fiji/schema/layout/foo-test.json";

  /** Test layout named 'foo' that uses RowKeyFormat with hash MD5 size 16 key settings.*/
  public static final String FOO_TEST_LEGACY =
      "com.moz.fiji/schema/layout/foo-test-legacy.json";

  /** Test layout named 'foo' with formatted entity IDs. */
  public static final String FOO_TEST_FORMATTED_EID =
      "com.moz.fiji/schema/layout/foo-test-formatted-eid.json";

  /** Test layout named 'ttl_test'. */
  public static final String TTL_TEST =
      "com.moz.fiji/schema/layout/ttl-test.json";

  /** Table named 'table' with a final string column named 'family:column'. */
  public static final String FINAL_COLUMN =
      "com.moz.fiji/schema/layout/final-column.json";

  /** Table named 'table' which uses formatted row keys. */
  public static final String FORMATTED_RKF =
      "com.moz.fiji/schema/layout/formattedkey.json";

  /** Table named 'table' which uses formatted row keys to express hashed keys. */
  public static final String HASHED_FORMATTED_RKF =
      "com.moz.fiji/schema/layout/hashed-formattedkey.json";

  /** Table named 'table' which uses hash prefixed row keys. */
  public static final String HASH_PREFIXED_RKF =
      "com.moz.fiji/schema/layout/hashprefixedkey.json";

  /** Table named 'table' which uses hash prefixed formatted row keys with multiple components. */
  public static final String HASH_PREFIXED_FORMATTED_MULTI_COMPONENT =
      "com.moz.fiji/schema/layout/hashed-formattedkey-multicomponent.json";

  /** Table named 'table' which contains two columns containing different types. */
  public static final String TWO_COLUMN_DIFFERENT_TYPES =
      "com.moz.fiji/schema/layout/two-column-different-types.json";

  /** Layout for table 'reader_schema' to test messing with reader schemas. */
  public static final String READER_SCHEMA_TEST =
      "com.moz.fiji/schema/layout/reader-schema.json";

  /** Layout for table 'schemaregtest' to test schema registration. */
  public static final String SCHEMA_REG_TEST =
      "com.moz.fiji/schema/layout/schema-reg-test.json";

  public static final String SIMPLE_MAP_TYPE =
      "com.moz.fiji/schema/layout/simple-map-type.json";

  /** Test layout with hashing disabled. */
  public static TableLayoutDesc getFooUnhashedTestLayout() throws IOException {
    final TableLayoutDesc desc = getLayout(FOO_TEST_LEGACY);
    desc.setName("foo_nonhashed");
    ((RowKeyFormat)desc.getKeysFormat()).setEncoding(RowKeyEncoding.RAW);
    return desc;
  }

  /** Test changing the row key hashing property. */
  public static TableLayoutDesc getFooChangeHashingTestLayout() throws IOException {
    final TableLayoutDesc desc = getLayout(FOO_TEST_LEGACY);
    ((RowKeyFormat)desc.getKeysFormat()).setEncoding(RowKeyEncoding.RAW);
    desc.setReferenceLayout("1");
    return desc;
  }

  /**
   * Layout to test simple kmeans.
   * The 'simple_kmeans_data' table has a single column family 'info' with two columns in it:
   * <ul>
   *   <li>username - The user's name.</li>
   *   <li>height - The height of a person</li>
   *   <li>weight - The weight of this person</li>
   * </ul>
   */
  public static final String SIMPLE_KMEANS_TEST =
      "com.moz.fiji/schema/layout/simple-kmeans-test.json";

  /** Layout to test deletes. */
  public static final String DELETES_TEST =
      "com.moz.fiji/schema/layout/deletes-test.json";

  public static final String GATHER_MAP_TEST =
      "com.moz.fiji/schema/layout/gather-map-test.json";

  public static final String FOODS =
      "com.moz.fiji/schema/layout/foods.json";

  /** Layout to test regex filters. */
  public static final String REGEX =
      "com.moz.fiji/schema/layout/regex.json";

  /** Layout with invalid schema. */
  public static final String INVALID_SCHEMA =
      "com.moz.fiji/schema/layout/invalid-schema.json";

  /** Layout to test bulk loads during setup and cleanup methods. */
  public static final String SETUP_CLEANUP_TABLE_TEST =
      "com.moz.fiji/schema/layout/setup-cleanup-table-test.json";

  /** Layout to test the identity producer. */
  public static final String IDENTITY_PRODUCER_TEST =
      "com.moz.fiji/schema/layout/identity-producer-test.json";

  /**
   * Creates a temporary JSON file with the specified layout.
   *
   * @param desc Layout descriptor.
   * @return Temporary JSON file containing the specified layout.
   * @throws IOException on I/O error.
   */
  public static File getTempFile(TableLayoutDesc desc) throws IOException {
    final File layoutFile = File.createTempFile("layout-" + desc.getName(), "json");
    layoutFile.deleteOnExit();
    final OutputStream fos = new FileOutputStream(layoutFile);
    IOUtils.write(ToJson.toJsonString(desc), fos);
    ResourceUtils.closeOrLog(fos);
    return layoutFile;
  }

  /** Disable constructor for this utility class. */
  private FijiTableLayouts() {}
}
