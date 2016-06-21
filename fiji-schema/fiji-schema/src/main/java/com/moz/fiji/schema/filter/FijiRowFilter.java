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

package com.moz.fiji.schema.filter;

import java.io.IOException;

import org.apache.hadoop.hbase.filter.Filter;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.annotations.Inheritance;
import com.moz.fiji.schema.DecodedCell;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiIOException;
import com.moz.fiji.schema.NoSuchColumnException;
import com.moz.fiji.schema.hbase.HBaseColumnName;

/**
 * The abstract base class for filters that exclude data from FijiRows.
 *
 * <p>Classes extending FijiRowFilter must implement the <code>hashCode</code> and
 * <code>equals</code> methods.</p>
 */
@ApiAudience.Public
@ApiStability.Evolving
@Inheritance.Extensible
public abstract class FijiRowFilter {
  /** The JSON node name used for the deserializer class. */
  private static final String DESERIALIZER_CLASS_NODE = "filterDeserializerClass";

  /** The JSON node name used for the filter fields. */
  private static final String FILTER_NODE = "filter";

  /**
   * Deserialize a {@code FijiRowFilter} from JSON that has been constructed
   * using {@link #toJson}.
   *
   * @param json A JSON String created by {@link #toJson}
   * @return A {@code FijiRowFilter} represented by the JSON
   * @throws FijiIOException in case the json cannot be read or the filter
   *     cannot be instantiated
   */
  public static FijiRowFilter toFilter(String json) {
    final ObjectMapper mapper = new ObjectMapper();
    try {
      final JsonNode root = mapper.readTree(json);
      return toFilter(root);
    } catch (IOException ioe) {
      throw new FijiIOException(ioe);
    }
  }

  /**
   * Deserialize a {@code FijiRowFilter} from JSON that has been constructed
   * using {@link #toJson}.
   *
   * @param root A {@code JsonNode} created by {@link #toJson}
   * @return A {@code FijiRowFilter} represented by the JSON
   * @throws FijiIOException in case the filter cannot be instantiated
   */
  public static FijiRowFilter toFilter(JsonNode root) {
    final String filterDeserializerClassName = root.path(DESERIALIZER_CLASS_NODE).getTextValue();
    try {
      final Class<?> filterDeserializerClass = Class.forName(filterDeserializerClassName);
      final FijiRowFilterDeserializer filterDeserializer =
          (FijiRowFilterDeserializer) filterDeserializerClass.newInstance();
      final FijiRowFilter filter = filterDeserializer.createFromJson(root.path(FILTER_NODE));
      return filter;
    } catch (ClassNotFoundException cnfe) {
      throw new FijiIOException(cnfe);
    } catch (IllegalAccessException iae) {
      throw new FijiIOException(iae);
    } catch (InstantiationException ie) {
      throw new FijiIOException(ie);
    }
  }

  /**
   * A helper class for converting between Fiji objects and their HBase counterparts.
   */
  @ApiAudience.Public
  @Inheritance.Sealed
  public abstract static class Context {
    /**
     * Converts a Fiji row key into an HBase row key.
     *
     * <p>This method is useful only on tables with row key hashing disabled, since hashed
     * row keys have no ordering or semantics beyond being an identifier.</p>
     *
     * @param fijiRowKey A fiji row key.
     * @return The corresponding HBase row key.
     */
    public abstract byte[] getHBaseRowKey(String fijiRowKey);

    /**
     * Converts a Fiji column name to an HBase column family name.
     *
     * <p>Fiji optimizes cell storage in HBase by mapping user-provided column names to
     * compact identifiers (usually just a single byte). For example, what you refer to as
     * a column name "my_fiji_column" might actually be stored in HBase as a column name
     * with a single letter "B". Because of this, FijiRowFilter implementations that
     * reference Fiji column names should use this method to obtain the name of corresponding
     * optimized HBase column name.</p>
     *
     * @param fijiColumnName The name of a fiji column.
     * @return The name of the HBase column that stores the fiji column data.
     * @throws NoSuchColumnException If there is no such column in the fiji table.
     */
    public abstract HBaseColumnName getHBaseColumnName(FijiColumnName fijiColumnName)
        throws NoSuchColumnException;

    /**
     * Converts a Fiji cell value into an HBase cell value.
     *
     * <p>Fiji stores bytes into HBase cells by encoding an identifier for the writer schema
     * followed by the binary-encoded Avro data. Because of this, FijiRowFilter implementations
     * that inspect the contents of a cell should use this method to obtain the bytes encoded
     * as they would be stored by Fiji.</p>
     *
     * <p>For example, if you are implementing a FijiRowFilter that only considers rows
     * where column C contains an integer value of 123, construct an HBase Filter that
     * checks whether the contents of the cell in <code>getHBaseColumnName(C)</code> equals
     * <code>getHBaseCellValue(C, DecodedCell(<i>INT</i>, 123))</code>.</p>
     *
     * @param column Name of the column this cell belongs to.
     * @param fijiCell A fiji cell value.
     * @return The Fiji cell encoded as an HBase value.
     * @throws IOException If there is an error encoding the cell value.
     */
    public abstract byte[] getHBaseCellValue(FijiColumnName column, DecodedCell<?> fijiCell)
        throws IOException;
  }

  /**
   * Describes the data the filter requires to determine whether a row should be accepted.
   *
   * @return The data request.
   */
  public abstract FijiDataRequest getDataRequest();

  /**
   * Constructs an HBase <code>Filter</code> instance that can be used to instruct the
   * HBase region server which rows to filter.
   *
   * <p>You must use the given <code>context</code> object when referencing any HBase
   * table coordinates or values.  Using a Fiji row key, column family name, or column
   * qualifier name when configuring an HBase filter will result in undefined
   * behavior.</p>
   *
   * <p>For example, when constructing an HBase <code>SingleColumnValueFilter</code> to
   * inspect the "info:name" column of a Fiji table, use {@link
   * FijiRowFilter.Context#getHBaseColumnName(FijiColumnName)} to
   * retrieve the HBase column family and qualifier.  Here's an implementation that
   * filters out rows where the latest version of the 'info:name' is equal to 'Bob'.
   *
   * <pre>
   * FijiCell&lt;CharSequence&gt; bobCellValue = new FijiCell&lt;CharSequence&gt;(
   *     Schema.create(Schema.Type.STRING), "Bob");
   * HBaseColumnName hbaseColumn = context.getHBaseColumnName(
   *     FijiColumnName.create("info", "name"));
   * SingleColumnValueFilter filter = new SingleColumnValueFilter(
   *     hbaseColumn.getFamily(),
   *     hbaseColumn.getQualifier(),
   *     CompareOp.NOT_EQUAL,
   *     context.getHBaseCellValue(bobCellValue));
   * filter.setLatestVersionOnly(true);
   * filter.setFilterIfMissing(false);
   * return new SkipFilter(filter);</pre>
   * </p>
   *
   * @param context A helper object you can use to convert Fiji objects into their HBase
   *     counterparts.
   * @return The HBase filter the implements the semantics of this FijiRowFilter.
   * @throws IOException If there is an error.
   */
  public abstract Filter toHBaseFilter(Context context) throws IOException;

  /**
   * Constructs a {@code JsonNode} that describes the filter so that it may be
   * serialized.
   *
   * @return A {@code JsonNode} describing the filter
   */
  public final JsonNode toJson() {
    ObjectNode root = JsonNodeFactory.instance.objectNode();
    root.put(DESERIALIZER_CLASS_NODE, getDeserializerClass().getName());
    root.put(FILTER_NODE, toJsonNode());
    return root;
  }

  /**
   * Constructs a {@code JsonNode} that holds the data structures specific to
   * the filter.  Implementing classes should include in the return node only
   * their own fields.
   *
   * @return A {@code JsonNode} containing the filter's fields
   */
  protected abstract JsonNode toJsonNode();

  /**
   * Returns {@code Class} that is responsible for deserializing the filter.
   * Subclasses that want to have non-default constructors with final fields can
   * define a different class to deserialize it; typically the deserializer
   * class will be a static inner class.
   *
   * @return a {@code FijiRowFilterDeserializer} class that will be responsible
   *     for deserializing this filter
   */
  protected abstract Class<? extends FijiRowFilterDeserializer> getDeserializerClass();
}
