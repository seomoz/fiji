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

package com.moz.fiji.hive;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.hive.io.FijiRowDataWritable;
import com.moz.fiji.hive.utils.FijiDataRequestSerializer;
import com.moz.fiji.schema.FijiColumnName;

/**
 * A serializer and deserializer for reading from and writing to Fiji tables in Hive.
 *
 * Main entry point for the Fiji Hive Adapter.
 */
public class FijiTableSerDe implements SerDe {
  private static final Logger LOG = LoggerFactory.getLogger(FijiTableSerDe.class);

  // Hive configuration property for defining the table name
  public static final String HIVE_TABLE_NAME_PROPERTY = "name";

  // Property for specifying which columns are used within a Hive view.
  public static final String LIST_COLUMN_EXPRESSIONS = "fiji.columns";

  // Property for specifying which column represents the EntityId's shell string.
  // Cannot be specified at the same time as LIST_ENTITY_ID_COMPONENTS.
  public static final String ENTITY_ID_SHELL_STRING = "fiji.entity.id.shell.string";

  // Property specifying a list of Hive columns which represent the EntityId.
  // Cannot be specified at the same time as ENTITY_ID_SHELL_STRING.
  // TODO(KIJIHIVE-30): this feature isn't yet supported, but can come as a later patch.
  // Make a ticket and prioritize it accordingly.
  public static final String LIST_ENTITY_ID_COMPONENTS = "fiji.entity.id.columns";

  public static final String KIJI_QUALIFIER_PAGING_PREFIX = "fiji.qualifier.paging.";
  public static final String KIJI_CELL_PAGING_PREFIX = "fiji.cell.paging.";

  /**
   * This contains all the information about a Hive table we need to interact with a Fiji table.
   */
  private HiveTableDescription mHiveTableDescription;

  /** {@inheritDoc} */
  @Override
  public void initialize(Configuration conf, Properties properties) throws SerDeException {
    // Read from the magic property that contains the hive table definition's column names.
    final List<String> columnNames = readPropertyList(properties, Constants.LIST_COLUMNS);

    // Read from the magic property that contains the hive table definition's column types.
    final String columnTypes = properties.getProperty(Constants.LIST_COLUMN_TYPES);

    // Read from a property we require that contains the expressions specifying the data to map.
    Preconditions.checkArgument(properties.containsKey(LIST_COLUMN_EXPRESSIONS),
        "SERDEPROPERTIES missing configuration for property: {}", LIST_COLUMN_EXPRESSIONS);
    final List<String> columnExpressions = readPropertyList(properties, LIST_COLUMN_EXPRESSIONS);

    // Check that at least one of LIST_ENTITY_ID_COMPONENTS or ENTITY_ID_SHELL_STRING is
    // unspecified.
    Preconditions.checkArgument(!properties.containsKey(ENTITY_ID_SHELL_STRING)
        || !properties.containsKey(LIST_ENTITY_ID_COMPONENTS),
        "SERDEPROPERTIES cannot specify both: %s and %x.",
        ENTITY_ID_SHELL_STRING,
        LIST_ENTITY_ID_COMPONENTS);

    // Read from an optional property that contains the shell string representing the EntityId to
    // write back to Fiji with.
    String entityIdShellString = properties.getProperty(ENTITY_ID_SHELL_STRING);

    Map<String, String> qualifierPagingMap =
        readPrefixedPropertyMap(properties, KIJI_QUALIFIER_PAGING_PREFIX);
    // Validate that everything in the qualifier paging map is not fully qualified and is thus a
    // family.
    for (String qualifierPagingColumn : qualifierPagingMap.keySet()) {
      FijiColumnName fijiColumnName = new FijiColumnName(qualifierPagingColumn);
      Preconditions.checkArgument(!fijiColumnName.isFullyQualified(),
          "Cannot page over qualifiers for a fully qualified column: %s",
          qualifierPagingColumn);
    }

    Map<String, String> cellPagingMap =
        readPrefixedPropertyMap(properties, KIJI_CELL_PAGING_PREFIX);
    // Validate that no fully qualified cell paging columns override a family cell paging
    // configuration.
    for (String cellPagingColumn : qualifierPagingMap.keySet()) {
      FijiColumnName fijiColumnName = new FijiColumnName(cellPagingColumn);
      if (fijiColumnName.isFullyQualified()) {
        Preconditions.checkArgument(!qualifierPagingMap.containsKey(fijiColumnName.getFamily()),
            "Cannot override family level cell paging with fully qualified cell paging: %s",
            fijiColumnName.toString());
      }
    }

    mHiveTableDescription = HiveTableDescription.newBuilder()
        .withColumnNames(columnNames)
        .withColumnTypes(TypeInfoUtils.getTypeInfosFromTypeString(columnTypes))
        .withColumnExpressions(columnExpressions)
        .withEntityIdShellStringColumn(entityIdShellString)
        .withQualifierPagingMap(qualifierPagingMap)
        .withCellPagingMap(cellPagingMap)
        .build();

    if (!mHiveTableDescription.isWritable()) {
      LOG.warn("Neither {} nor {} unspecified, so this Hive view of a FijiTable is read only.",
          ENTITY_ID_SHELL_STRING,
          LIST_ENTITY_ID_COMPONENTS);
    }

    final String hiveName = properties.getProperty("name");
    final String dataRequestParameter = FijiTableInputFormat.CONF_KIJI_DATA_REQUEST_PREFIX
        + hiveName;
    try {
      if (null == conf) {
        conf = new HBaseConfiguration();
      }
      conf.set(dataRequestParameter,
          FijiDataRequestSerializer.serialize(mHiveTableDescription.getDataRequest()));
    } catch (IOException e) {
      throw new SerDeException("Unable to construct the data request.", e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Class<? extends Writable> getSerializedClass() {
    return FijiRowDataWritable.class;
  }

  /** {@inheritDoc} */
  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    if (!mHiveTableDescription.isWritable()) {
      throw new SerDeException("FijiTable has no EntityId mapping and is not writable.");
    }
    try {
      return mHiveTableDescription.createWritableObject(obj, objInspector);
    } catch (IOException e) {
      throw new SerDeException("Error writing data from the HBase result", e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    final FijiRowDataWritable result = (FijiRowDataWritable) blob;
    try {
      return mHiveTableDescription.createDataObject(result);
    } catch (IOException e) {
      throw new SerDeException("Error reading data from the HBase result", e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return mHiveTableDescription.getObjectInspector();
  }

  /** {@inheritDoc} */
  @Override
  public SerDeStats getSerDeStats() {
    // We don't support statistics.
    return null;
  }

  /**
   * Reads a comma-separated list of strings from a properties object.
   *
   * @param properties The properties object to read from.
   * @param name The field name to read from.
   * @return A list of the comma-separated fields in the property value.
   */
  private static List<String> readPropertyList(Properties properties, String name) {
    return Arrays.asList(properties.getProperty(name).split(","));
  }

  /**
   * Reads a map of all properties starting with a prefix.
   *
   * @param properties The properties object to read from.
   * @param prefix The prefix to match the properties on.
   * @return A map of the suffixes to the corresponding properties.
   */
  private static Map<String, String> readPrefixedPropertyMap(Properties properties,
                                                             String prefix) {
    Map<String, String> prefixedPropertyMap = Maps.newHashMap();
    for (String propertyName : properties.stringPropertyNames()) {
      if (propertyName.startsWith(prefix)) {
        String suffix = propertyName.replaceFirst(prefix, "");
        String value = properties.getProperty(propertyName);
        prefixedPropertyMap.put(suffix, value);
      }
    }
    return prefixedPropertyMap;
  }
}
