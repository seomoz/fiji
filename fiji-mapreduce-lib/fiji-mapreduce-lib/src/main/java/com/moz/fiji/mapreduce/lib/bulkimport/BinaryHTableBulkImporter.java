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

package com.moz.fiji.mapreduce.lib.bulkimport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.hadoop.configurator.HadoopConf;
import com.moz.fiji.hadoop.configurator.HadoopConfigurator;
import com.moz.fiji.mapreduce.FijiTableContext;
import com.moz.fiji.mapreduce.bulkimport.FijiBulkImporter;
import com.moz.fiji.mapreduce.impl.HTableReader;
import com.moz.fiji.schema.DecodedCell;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiColumnName;

/**
 * Bulk Importer that handles binary data in an HBase table. This importer can be used when the
 * data has been encoded using the {@link org.apache.hadoop.hbase.util.Bytes} utility class
 *
 * <p>Since HTable columns have no concept of schemas or types, this importer requires you to
 * specify the type of encoding that was used for each input HTable column.  Any of the primitive
 * types supported by the HBase <code>Bytes</code> utility class may be used here:</p>
 *
 * <table summary="">
 *   <tr><th>HBase <i>input-type</i></th><th>Avro schema</th><th>Decoding method</th></tr>
 *   <tr><td>boolean</td><td>"boolean"</td><td>Bytes.toBoolean(byte[])</td></tr>
 *   <tr><td>bytes</td><td>"bytes"</td><td><i>none</i></td></tr>
 *   <tr><td>double</td><td>"double"</td><td>Bytes.toDouble(byte[])</td></tr>
 *   <tr><td>float</td><td>"float"</td><td>Bytes.toFloat(byte[])</td></tr>
 *   <tr><td>int</td><td>"int"</td><td>Bytes.toInt(byte[])</td></tr>
 *   <tr><td>long</td><td>"long"</td><td>Bytes.toLong(byte[])</td></tr>
 *   <tr><td>short</td><td>"int"</td><td>Bytes.toShort(byte[])</td></tr>
 *   <tr><td>string</td><td>"string"</td><td>Bytes.toString(byte[])</td></tr>
 * </table>
 *
 * <p>The most recent version of each input column (<i>hbase-column</i>) will be
 * decoded using the input type (<i>input-type</i>) and stored into a target Fiji
 * column (<i>fiji-column</i>) using the same timestamp as the HBase cell.</p>
 *
 * <p>The configuration variable <i>binary.htable.importer.columns</i> should
 * contain a comma-separated list of <i>column-descriptor</i>s:</p>
 *
 * <p>
 * <i>hbase-column</i>:<i>input-type</i>:<i>fiji-column</i>
 * </p>
 *
 * <p>For example, to run a bulk import job that reads a string from the
 * <i>i:name</i> column and an integer from the <i>i:id</i> column of an HBase
 * table <i>mytable</i> into the <i>info:name</i> and <i>info:id</i>
 * columns of a Fiji table <i>fijitable</i>:</p>
 *
 * <pre>
 * $ fiji bulk-import \
 * &gt;   -D binary.htable.importer.columns=i:name:string:info:name,i:id:int:info:id \
 * &gt;   --importer=com.moz.fiji.mapreduce.lib.bulkimport.BinaryHTableBulkImporter \
 * &gt;   --input=htable:mytable \
 * &gt;   --table=fijitable
 * </pre>
 *
 * <p>Note: Like all bulk importers, only one column family may be targeted at a time.  To
 * import into multiple column families, run the importer once per target family.</p>
 *
 */
@ApiAudience.Public
public final class BinaryHTableBulkImporter
  extends FijiBulkImporter<ImmutableBytesWritable, Result>
  implements HTableReader {

  private static final Logger LOG = LoggerFactory.getLogger(BinaryHTableBulkImporter.class);

  /** The configuration variable listing the input/output columns and their types. */
  public static final String CONF_COLUMN_DESCRIPTORS = "binary.htable.importer.columns";

  /**
   * A list of objects that describe the input HTable columns and how to convert them to
   * Fiji table cells.
   */
  private List<ColumnDescriptor> mColumnDescriptors;

  /**
   * Decodes HBase cell data that was encoded using the
   * {@link org.apache.hadoop.hbase.util.Bytes} class.
   */
  private BinaryHBaseCellDecoder mHBaseCellDecoder;

  /** Constructor. */
  public BinaryHTableBulkImporter() {
  }

  /** {@inheritDoc} */
  @Override
  public Scan getInputHTableScan(Configuration conf) throws IOException {
    // Add all the input HTable columns to the scan descriptor to they are passed to us
    // in the produce() method.
    Scan scan = new Scan();
    for (ColumnDescriptor columnDescriptor : mColumnDescriptors) {
      columnDescriptor.addToScan(scan);
    }
    scan.setCacheBlocks(false);
    return scan;
  }

  /** {@inheritDoc} */
  @Override
  public void setup(FijiTableContext context) throws IOException {
    mHBaseCellDecoder = new BinaryHBaseCellDecoder();
  }

  /** {@inheritDoc} */
  @Override
  public void produce(ImmutableBytesWritable hbaseRowKey, Result hbaseRow, FijiTableContext context)
      throws IOException {

    EntityId entity = context.getEntityId(Bytes.toString(hbaseRowKey.get()));
    for (ColumnDescriptor columnDescriptor : mColumnDescriptors) {
      KeyValue keyValue = hbaseRow.getColumnLatest(columnDescriptor.getHBaseFamilyBytes(),
          columnDescriptor.getHBaseQualifierBytes());
      if (null == keyValue) {
        // No data in this HTable column, skip it.
        continue;
      }

      // Convert the HBase cell to a Fiji cell.
      DecodedCell<?> fijiCell = decodeHBaseCell(columnDescriptor, keyValue.getValue());

      // Write it at the same timestamp as the HBase cell.
      final String family = columnDescriptor.getFijiColumnName().getFamily();
      final String qualifier = columnDescriptor.getFijiColumnName().getQualifier();
      context.put(entity, family, qualifier, keyValue.getTimestamp(), fijiCell.getData());
    }
  }

  /**
   * Initializes this object's state using the settings from the configuration.
   * Throws a RuntimeException if there is an error.
   *
   * @param conf The configuration to read settings from.
   */
  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    HadoopConfigurator.configure(this);
  }

  /**
   * Sets the column descriptors.
   *
   * @param columnDescriptors The list of descriptors.
   */
  @HadoopConf(key=CONF_COLUMN_DESCRIPTORS)
  protected void setColumnDescriptors(String[] columnDescriptors) {
    if (null == columnDescriptors || 0 == columnDescriptors.length) {
      throw new RuntimeException("Configuration variable " + CONF_COLUMN_DESCRIPTORS
          + " was not set. Try using -D" + CONF_COLUMN_DESCRIPTORS + "=...");
    }

    mColumnDescriptors = new ArrayList<ColumnDescriptor>();
    for (String columnDescriptor : columnDescriptors) {
      try {
        mColumnDescriptors.add(ColumnDescriptor.parse(columnDescriptor));
      } catch (IOException ioe) {
        LOG.error(ioe.getMessage());
        throw new RuntimeException(ioe);
      }
    }

    // A non-empty column descriptor string should have parsed to at least one descriptor.
    assert !mColumnDescriptors.isEmpty();

    // The output fiji columns must all be in the same family.
    String family = mColumnDescriptors.get(0).getFijiColumnName().getFamily();
    for (ColumnDescriptor columnDescriptor : mColumnDescriptors) {
      if (!family.equals(columnDescriptor.getFijiColumnName().getFamily())) {
        throw new RuntimeException("Only one column family may be written to at a time."
            + " Attempted to write to " + family + " and "
            + columnDescriptor.getFijiColumnName().getFamily() + "."
            + " To import into multiple column families,"
            + " run one import job per output column family.");
      }
    }
  }

  /**
   * Describes an input HTable column, its type, and a the target column in a Fiji table
   * it should be imported into.
   */
  protected static final class ColumnDescriptor {
    /** The input HBase column family. */
    private final String mHBaseFamily;
    /** The input HBase column qualifier. */
    private final String mHBaseQualifier;
    /** The type of the HBase column (used to determine how to decode the bytes). */
    private final String mHBaseType;
    /** The name of the target Fiji column. */
    private final FijiColumnName mFijiColumnName;

    /**
     * Creates a new <code>ColumnDescriptor</code> instance.
     *
     * @param hbaseFamily The input HBase column family.
     * @param hbaseQualifier The input HBase column qualifier.
     * @param hbaseType The type of the HBase column (used to determine how to decode the bytes).
     * @param fijiColumnName The name of the target Fiji column.
     */
    private ColumnDescriptor(String hbaseFamily, String hbaseQualifier, String hbaseType,
        FijiColumnName fijiColumnName) {
      mHBaseFamily = hbaseFamily;
      mHBaseQualifier = hbaseQualifier;
      mHBaseType = hbaseType;
      mFijiColumnName = fijiColumnName;
    }



    /**
     * Gets the input HBase column family.
     *
     * @return The input HBase column family.
     */
    public byte[] getHBaseFamilyBytes() {
      return Bytes.toBytes(mHBaseFamily);
    }

    /**
     * Gets the input HBase column qualifier.
     *
     * @return The input HBase column qualifier.
     */
    public byte[] getHBaseQualifierBytes() {
      return Bytes.toBytes(mHBaseQualifier);
    }

    /**
     * Gets the type of the HBase column, used to determine how to decode the cell's bytes.
     *
     * @return The type of the HBase column.
     */
    public String getType() {
      return mHBaseType;
    }

    /**
     * Gets the name of the target Fiji column.
     *
     * @return The name of the target Fiji column.
     */
    public FijiColumnName getFijiColumnName() {
      return mFijiColumnName;
    }

    /**
     * Adds a request for the HBase column to the scan descriptor.
     *
     * @param scan The scan to add the request to.
     */
    public void addToScan(Scan scan) {
      scan.addColumn(getHBaseFamilyBytes(), getHBaseQualifierBytes());
    }

    /**
     * Parses an input string into a ColumnDescriptor.
     *
     * <p>The input string must be formatted as:</p>
     *
     * <code>hbase-family:hbase-qualifier:hbase-type:fiji-family:fiji-qualifier</code>
     *
     * @param input The input string.
     * @return A parsed ColumnDescriptor.
     * @throws IOException If the input string cannot be parsed.
     */
    public static ColumnDescriptor parse(String input) throws IOException {
      String[] parts = StringUtils.splitByWholeSeparatorPreserveAllTokens(input, ":", 4);
      if (parts.length < 4) {
        throw new IOException("Expected at least 4 colon-separated fields, but got " + parts.length
            + " while parsing " + input);
      }
      FijiColumnName fijiColumnName = new FijiColumnName(parts[3]);
      if (!fijiColumnName.isFullyQualified()) {
        throw new IOException("Output fiji columns must have a family and qualifier.");
      }
      return new ColumnDescriptor(parts[0], parts[1], parts[2], fijiColumnName);
    }
  }

  /**
   * Converts the contents of an HBase cell into a typed Fiji cell.
   *
   * @param columnDescriptor Describes the HBase column the cell came from.
   * @param hbaseCell The bytes of the HBase cell to be converted.
   * @return A fiji cell to be imported.
   * @throws IOException If there is an error.
   */
  protected DecodedCell<?> decodeHBaseCell(ColumnDescriptor columnDescriptor, byte[] hbaseCell)
      throws IOException {
    BinaryHBaseCellDecoder.Type type = BinaryHBaseCellDecoder.Type.valueOf(
        columnDescriptor.getType().toUpperCase(Locale.getDefault()));
    return mHBaseCellDecoder.decode(type, hbaseCell);
  }

  /**
   * Decodes the bytes of an HBase cell that was encoded using
   * {@link org.apache.hadoop.hbase.util.Bytes}.
   */
  protected static final class BinaryHBaseCellDecoder {
    /** The supported HBase types that can be decoded. */
    public static enum Type {
      BOOLEAN(Schema.Type.BOOLEAN),
      BYTES(Schema.Type.BYTES),
      DOUBLE(Schema.Type.DOUBLE),
      FLOAT(Schema.Type.FLOAT),
      INT(Schema.Type.INT),
      LONG(Schema.Type.LONG),
      SHORT(Schema.Type.INT),
      STRING(Schema.Type.STRING);

      /** The Avro schema. */
      private final Schema mAvroSchema;

      /**
       * Constructor.
       *
       * @param avroSchemaType The type of Avro schema this decodes into.
       */
      Type(Schema.Type avroSchemaType) {
        mAvroSchema = Schema.create(avroSchemaType);
      }

      /**
       * The type of the decoded Fiji cell.
       *
       * @return The schema of the decoded Fiji cell.
       */
      public Schema getSchema() {
        return mAvroSchema;
      }
    }

    /**
     * Decodes an HBase cell's bytes into a Fiji cell.
     *
     * @param type The type of the data encoded into the HBase cell bytes.
     * @param bytes The bytes to decode.
     * @return The decoded fiji cell.
     * @throws IOException If there is an error decoding.
     */
    public DecodedCell<?> decode(Type type, byte[] bytes) throws IOException {
      switch(type) {
      case BOOLEAN:
        return new DecodedCell<Boolean>(type.getSchema(), Bytes.toBoolean(bytes));
      case BYTES:
        return new DecodedCell<ByteBuffer>(type.getSchema(), ByteBuffer.wrap(bytes));
      case DOUBLE:
        return new DecodedCell<Double>(type.getSchema(), Bytes.toDouble(bytes));
      case FLOAT:
        return new DecodedCell<Float>(type.getSchema(), Bytes.toFloat(bytes));
      case INT:
        return new DecodedCell<Integer>(type.getSchema(), Bytes.toInt(bytes));
      case LONG:
        return new DecodedCell<Long>(type.getSchema(), Bytes.toLong(bytes));
      case SHORT:
        return new DecodedCell<Integer>(type.getSchema(), (int) Bytes.toShort(bytes));
      case STRING:
        return new DecodedCell<CharSequence>(type.getSchema(), Bytes.toString(bytes));
      default:
        throw new IOException("Unsupported HBase encoding type: " + type.toString());
      }
    }
  }
}
