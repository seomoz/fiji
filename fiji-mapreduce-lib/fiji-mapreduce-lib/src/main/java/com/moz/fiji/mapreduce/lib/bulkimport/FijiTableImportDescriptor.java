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

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.mapreduce.lib.avro.ColumnDesc;
import com.moz.fiji.mapreduce.lib.avro.FamilyDesc;
import com.moz.fiji.mapreduce.lib.avro.TableImportDescriptorDesc;
import com.moz.fiji.mapreduce.lib.bulkimport.FijiTableImportDescriptor.FamilyLayout.ColumnLayout;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.util.FromJson;
import com.moz.fiji.schema.util.FijiNameValidator;
import com.moz.fiji.schema.util.ProtocolVersion;
import com.moz.fiji.schema.util.ResourceUtils;
import com.moz.fiji.schema.util.ToJson;

/**
 * Mapping of input schema to Fiji table layouts for bulk importers.
 *
 * <p>
 *   FijiTableImportDescriptor wraps a table import descriptor represented as a
 *   {@link com.moz.fiji.mapreduce.lib.avro.TableImportDescriptorDesc TableImportDescriptorDesc} Avro
 *   record.  FijiTableImportDescriptor provides validation and accessors to navigate
 *   through the mapping.
 * </p>
 *
 * Sample table import descriptor:<pre><code>
 * {
 *   name : "foo", // destination table of the import
 *   families : [ {
 *     name : "info" // column family for the import
 *     columns : [ {
 *       name : "first_name", // name of the column within the column family
 *       source : "first" // field in the source to import from
 *     }, {
 *      name : "last_name",
 *      source : "last"
 *     } ],
 *   } ],
 *   entityIdSource : "first", // field in the source to generate the entity id from.
 *   overrideTimestampSource : "time", // optional field to use to set the timestamps on rows.
 *   version : "import-1.0" // format version number of the import descriptor
 * }
 * </code></pre>
 *
 * <h2>Overall structure</h2>
 * <p>At the top-level, a table import descriptor contains:</p>
 * <ul>
 *   <li>the table that is the destination of the import.</li>
 *   <li>the table column families.</li>
 *   <li>the source for the entity id.</li>
 *   <li>(optional) timestamp to use instead of system timestamp.</li>
 *   <li>format version of the import descriptor.</li>
 * </ul>
 *
 * <p>Each column family has:</p>
 * <ul>
 *   <li>the name of the destination column.</li>
 *   <li>the name of the source field to import from.</li>
 * </ul>
 */
@ApiAudience.Public
public final class FijiTableImportDescriptor {
  private static final Logger LOG = LoggerFactory.getLogger(FijiTableImportDescriptor.class);

  // ProtocolVersions specifying when different features were added to import functionality.

  /** Minimum import version we can recognize. */
  private static final ProtocolVersion MIN_IMPORT_VER = ProtocolVersion.parse("import-1.0.0");

  /** Maximum import version we can recognize. */
  private static final ProtocolVersion MAX_IMPORT_VER = ProtocolVersion.parse("import-1.0.0");

  /** All import versions must use the format 'import-x.y' to specify what version they use. */
  private static final String IMPORT_PROTOCOL_NAME = "import";

  /** Concrete layout of a family. */
  @ApiAudience.Public
  public static final class FamilyLayout {

    /** Concrete layout of a column. */
    @ApiAudience.Public
    public final class ColumnLayout {
      /** Column name. */
      private final String mName;

      /** Column source. */
      private final String mSource;

      /**
       * Builds a new column layout instance from a descriptor.
       *
       * @param desc Column descriptor.
       * @throws InvalidTableImportDescriptorException if the layout is invalid or inconsistent.
       */
      public ColumnLayout(ColumnDesc desc)
          throws InvalidTableImportDescriptorException {
        mName = desc.getName();
        mSource = desc.getSource();

        if (!isValidName(desc.getName())) {
          throw new InvalidTableImportDescriptorException(String.format(
              "Invalid column name: '%s'.", desc.getName()));
        }
      }

      /** @return the name for the Fiji column. */
      public String getName() {
        return mName;
      }

      /** @return the import source for the column. */
      public String getSource() {
        return mSource;
      }

      /** @return the family this column belongs to. */
      public FamilyLayout getFamily() {
        return FamilyLayout.this;
      }
    }  // class ColumnLayout

    // -------------------------------------------------------------------------------------------

    /** Family layout descriptor. */
    private final FamilyDesc mDesc;

    /** Columns in the family. */
    private final ImmutableList<ColumnLayout> mColumns;

    /** Map column qualifier name (no aliases) to column layout. */
    private final ImmutableMap<String, ColumnLayout> mColumnMap;

    /**
     * Builds a new family layout instance.
     *
     * @param familyDesc Descriptor of the family.
     * @throws InvalidTableImportDescriptorException if the layout is invalid or inconsistent.
     */
    public FamilyLayout(FamilyDesc familyDesc)
        throws InvalidTableImportDescriptorException {
      mDesc = Preconditions.checkNotNull(familyDesc);

      // Ensure the array of columns is mutable:
      mDesc.setColumns(Lists.newArrayList(mDesc.getColumns()));

      if (!isValidName(familyDesc.getName())) {
        throw new InvalidTableImportDescriptorException(String.format(
            "Invalid family name: '%s'.", familyDesc.getName()));
      }

      // Build columns:

      final List<ColumnLayout> columns = Lists.newArrayList();
      final Map<String, ColumnLayout> columnMap = Maps.newHashMap();

      final Iterator<ColumnDesc> itColumnDesc = familyDesc.getColumns().iterator();
      while (itColumnDesc.hasNext()) {
        final ColumnDesc columnDesc = itColumnDesc.next();
        final ColumnLayout cLayout = new ColumnLayout(columnDesc);
        columns.add(cLayout);
        if (null != columnMap.put(cLayout.getName(), cLayout)) {
            throw new InvalidTableImportDescriptorException(String.format(
                "Family '%s' contains duplicate column qualifier '%s'.",
                getName(), cLayout.getName()));
        }
      }

      mColumns = ImmutableList.copyOf(columns);
      mColumnMap = ImmutableMap.copyOf(columnMap);
    }

    /** @return the primary name for the family. */
    public String getName() {
      return mDesc.getName();
    }

    /** @return the columns in this family. */
    public Collection<ColumnLayout> getColumns() {
      return mColumns;
    }

    /** @return the mapping from column names (no aliases) to column layouts. */
    public Map<String, ColumnLayout> getColumnMap() {
      return mColumnMap;
    }

  }  // class FamilyLayout

  // -----------------------------------------------------------------------------------------------

  /** Avro record describing the table layout absolutely (no reference layout required). */
  private final TableImportDescriptorDesc mDesc;

  /** Column name to import source mapping. */
  private ImmutableMap<FijiColumnName, String> mColumnNameToSource;

  /**
   * Constructs a FijiTableImportDescriptor from an Avro descriptor.
   *
   * @param desc Avro layout descriptor (relative to the reference layout).
   * @throws InvalidTableImportDescriptorException if the descriptor is invalid or inconsistent
   *   wrt reference.
   */
  public FijiTableImportDescriptor(TableImportDescriptorDesc desc)
      throws InvalidTableImportDescriptorException {

    // Deep-copy the descriptor to prevent mutating a parameter:
    mDesc = TableImportDescriptorDesc.newBuilder(Preconditions.checkNotNull(desc)).build();

    // Ensure the array of locality groups is mutable:
    mDesc.setFamilies(Lists.newArrayList(mDesc.getFamilies()));

    // Check that the version specified in the import descriptor matches the features used.
    final ProtocolVersion importVersion = ProtocolVersion.parse(mDesc.getVersion());
    if (!IMPORT_PROTOCOL_NAME.equals(importVersion.getProtocolName())) {
      throw new InvalidTableImportDescriptorException(
          String.format("Invalid version protocol: '%s'. Expected: '%s'.",
          importVersion.getProtocolName(),
          IMPORT_PROTOCOL_NAME));
    }

    if (MAX_IMPORT_VER.compareTo(importVersion) < 0) {
      throw new InvalidTableImportDescriptorException("The maximum import version we support is "
          + MAX_IMPORT_VER + "; this import requires " + importVersion);
    } else if (MIN_IMPORT_VER.compareTo(importVersion) > 0) {
      throw new InvalidTableImportDescriptorException("The minimum import version we support is "
          + MIN_IMPORT_VER + "; this import requires " + importVersion);
    }

    if (!isValidName(getName())) {
      throw new InvalidTableImportDescriptorException(
          String.format("Invalid table name: '%s'.", getName()));
    }

    /** All the families in the table. */
    final List<FamilyLayout> families = Lists.newArrayList();

    /** All primary column names mapped to their sources. */
    final Map<FijiColumnName, String> columnNameToSource = Maps.newTreeMap();

    final Map<FijiColumnName, ColumnLayout> columnMap = Maps.newHashMap();
    final Iterator<FamilyDesc> itFamilyDesc = mDesc.getFamilies().iterator();
    while (itFamilyDesc.hasNext()) {
      FamilyDesc familyDesc = itFamilyDesc.next();
      FamilyLayout familyLayout = new FamilyLayout(familyDesc);
      families.add(familyLayout);
      for (ColumnLayout columnLayout: familyLayout.getColumns()) {
        String columnName = columnLayout.getName();
        final FijiColumnName column = new FijiColumnName(familyLayout.getName(), columnName);
        if (null != columnMap.put(column, columnLayout)) {
          throw new InvalidTableImportDescriptorException(String.format(
              "Layout for table '%s' contains duplicate column '%s'.",
              getName(), column));
        }
        FijiColumnName fijiColumnName = new FijiColumnName(familyLayout.getName(),
            columnLayout.getName());
        columnNameToSource.put(fijiColumnName, columnLayout.getSource());
      }
    }

    mColumnNameToSource = ImmutableMap.copyOf(columnNameToSource);
  }

  /** @return the table name. */
  public String getName() {
    return mDesc.getName();
  }

  /** @return mapping of column names to the source in the derived files. */
  public Map<FijiColumnName, String> getColumnNameSourceMap() {
    return mColumnNameToSource;
  }

  /** @return the source for the timestamp. */
  public String getOverrideTimestampSource() {
    return mDesc.getOverrideTimestampSource();
  }

  /** @return the source for the entityId. */
  public String getEntityIdSource() {
    return mDesc.getEntityIdSource();
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof FijiTableImportDescriptor)) {
      return false;
    }
    final FijiTableImportDescriptor otherMapping = (FijiTableImportDescriptor) other;
    return mDesc.equals(otherMapping.mDesc);
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return mDesc.hashCode();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    try {
      return ToJson.toJsonString(mDesc);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Validates a name (table name, locality group name, family name, or column name).
   *
   * @param name The name to validateDestination.
   * @return whether the name is valid.
   */
  private static boolean isValidName(String name) {
    return FijiNameValidator.isValidLayoutName(name);
  }

  /**
   * Loads a table import mapping from the specified resource as JSON.
   *
   * @param resource Path of the resource containing the JSON layout description.
   * @return the parsed table layout.
   * @throws IOException on I/O error.
   */
  public static FijiTableImportDescriptor createFromEffectiveJsonResource(String resource)
      throws IOException {
    return createFromEffectiveJson(FijiTableImportDescriptor.class.getResourceAsStream(resource));
  }

  /**
   * Loads a table import mapping from the specified JSON text.  The InputStream passed into this
   * method is closed upon completion.
   *
   * @param istream Input stream containing the JSON text.
   * @return the parsed table layout.
   * @throws IOException on I/O error.
   */
  public static FijiTableImportDescriptor createFromEffectiveJson(InputStream istream)
      throws IOException {
    try {
      final TableImportDescriptorDesc desc = readTableImportMappingDescFromJSON(istream);
      final FijiTableImportDescriptor layout = new FijiTableImportDescriptor(desc);
      return layout;
    } finally {
      ResourceUtils.closeOrLog(istream);
    }
  }

  /**
   * Reads a table import mapping descriptor from its JSON serialized form.
   *
   * @param istream JSON input stream.
   * @return the decoded table layout descriptor.
   * @throws IOException on I/O error.
   */
  public static TableImportDescriptorDesc readTableImportMappingDescFromJSON(InputStream istream)
      throws IOException {
    final String json = IOUtils.toString(istream);
    return readTableImportDescriptorDescFromJSON(json);
  }

  /**
   * Reads a table import mapping descriptor from its JSON serialized form.
   *
   * @param json JSON string.
   * @return the decoded table layout descriptor.
   * @throws IOException on I/O error.
   */
  public static TableImportDescriptorDesc readTableImportDescriptorDescFromJSON(String json)
      throws IOException {
    final TableImportDescriptorDesc desc =
        (TableImportDescriptorDesc) FromJson.fromJsonString(json,
            TableImportDescriptorDesc.SCHEMA$);
    return desc;
  }

  /**
   * Validates that this table import descriptor can import data into the specified layout.  This
   * is done by ensuring that every destination column exists in the specified table's layout.
   *
   * @param tableLayout the table layout to validateDestination against
   * @throws InvalidTableImportDescriptorException if the import mapping can not be applied to the
   *     table layout
   */
  public void validateDestination(FijiTableLayout tableLayout)
      throws InvalidTableImportDescriptorException {
    Set<FijiColumnName> columnNames = tableLayout.getColumnNames();
    for (FijiColumnName columnName : getColumnNameSourceMap().keySet()) {
      if (!columnNames.contains(columnName)) {
        throw new InvalidTableImportDescriptorException(
            String.format("Table '%s' does not contain column '%s'.",
                tableLayout.getName(),
                columnName));
      }
    }
  }

}
