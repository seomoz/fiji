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
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.Inheritance;
import com.moz.fiji.hadoop.configurator.HadoopConf;
import com.moz.fiji.hadoop.configurator.HadoopConfigurator;
import com.moz.fiji.mapreduce.FijiTableContext;
import com.moz.fiji.mapreduce.bulkimport.FijiBulkImporter;
import com.moz.fiji.mapreduce.framework.JobHistoryCounters;
import com.moz.fiji.mapreduce.framework.FijiConfKeys;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiSchemaTable;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.avro.AvroSchema;
import com.moz.fiji.schema.avro.CellSchema;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.util.ResourceUtils;

/**
 * DescribedInputTextBulkImporter is an abstract class that provides methods to bulk importers for
 * mapping from source fields in the import lines to destination Fiji columns.  These can be used
 * inside of FijiBulkImportJobBuilder via the withBulkImporter method.
 *
 * <p>Importing from a text file requires specifying a FijiColumnName, and the source field
 * for each element to be inserted into Fiji, in addition to the raw import data.  This information
 * is provided by {@link FijiTableImportDescriptor} which is set via
 * the <code>fiji.import.text.input.descriptor.path</code> parameter in {@link #CONF_FILE}.
 *
 * <p>Use this Mapper over text files to import data into a Fiji
 * table.  Each line in the file will be treated as data for one row.
 * This line should generate a single EntityId to write to, and any number
 * of writes to add to that entity.  You should override the produce(String, Context)
 * method to generate the entities from the input lines.</p>
 *
 * <p>Extensions of this class should implement the following methods:
 * <ul>
 *   <li>{@link #produce} - actual producer code for the bulk importer should go here</li>
 *   <li>{@link #setupImporter} - (optional) any specific setup for this bulk importer.</li>
 *   <li>{@link #cleanupImporter} - (optional) any specific cleanup for this bulk importer.</li>
 * </ul>
 *
 * <p>Extensions of this class can use the following methods to implement their producers:
 * <ul>
 *   <li>{@link #convert} - parses the text into the type associated with the column.</li>
 *   <li>{@link #incomplete} - to log and mark a row that was incomplete.</li>
 *   <li>{@link #reject} - to log and mark a row that could not be processed.</li>
 *   <li>{@link #getDestinationColumns} - to retrieve a collection of destination columns.</li>
 *   <li>{@link #getSource} - to retrieve the source for one of the columns listed above.</li>
 *   <li>{@link #getEntityIdSource()} - to retrieve the source for the entity id for the row.</li>
 * </ul>
 */

@ApiAudience.Public
@Inheritance.Extensible
public abstract class DescribedInputTextBulkImporter extends FijiBulkImporter<LongWritable, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(DescribedInputTextBulkImporter.class);

  /**
   * Location of writer layout file.  File names columns and schemas, and implies
   * ordering of columns in delimited read-in file.
   */
  public static final String CONF_FILE = "fiji.import.text.input.descriptor.path";

  public static final String CONF_LOG_RATE = "fiji.import.text.log.rate";

  private static final ImmutableMap<String, Class<?>> FIJI_CELL_TYPE_TO_CLASS_MAP =
      new ImmutableMap.Builder<String, Class<?>>()
          .put("\"boolean\"", Boolean.class)
          .put("\"int\"", Integer.class)
          .put("\"long\"", Long.class)
          .put("\"float\"", Float.class)
          .put("\"double\"", Double.class)
          .put("\"string\"", String.class)
          .build();

  private static final ImmutableMap<Schema.Type, Class<?>> FIJI_AVRO_TYPE_TO_CLASS_MAP =
      new ImmutableMap.Builder<Schema.Type, Class<?>>()
          .put(Schema.Type.BOOLEAN, Boolean.class)
          .put(Schema.Type.INT, Integer.class)
          .put(Schema.Type.LONG, Long.class)
          .put(Schema.Type.FLOAT, Float.class)
          .put(Schema.Type.DOUBLE, Double.class)
          .put(Schema.Type.STRING, String.class)
          .build();

  /** Number of lines to skip between reject/incomplete lines. */
  private Long mLogRate = 1000L;

  /** Current counter of the number of incomplete lines. */
  private Long mIncompleteLineCounter = 0L;

  /** Current counter of the number of rejected lines. */
  private Long mRejectedLineCounter = 0L;

  /** Table layout of the output table. */
  private FijiTableLayout mOutputTableLayout;

  /** Table import descriptor for this bulk load. */
  private FijiTableImportDescriptor mTableImportDescriptor;

  /** FijiColumnName to cell type map. */
  private Map<FijiColumnName, Class> mColumnNameClassMap;

  /**
   * {@inheritDoc}
   *
   * <p>If you override this method, you must call <tt>super.setConf(conf)</tt>.</p>
   */
  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
  }

  /**
   * Performs validation that this table import descriptor can be applied to the output table.
   *
   * This method is final to prevent it from being overridden without being called.
   * Subclasses should override the {@link #setupImporter} method instead of overriding this method.
   *
   * {@inheritDoc}
   */
  @Override
  public final void setup(FijiTableContext context) throws IOException {
    HadoopConfigurator.configure(this);
    final Configuration conf = getConf();
    Preconditions.checkNotNull(mTableImportDescriptor);

    final FijiURI uri = FijiURI.newBuilder(conf.get(FijiConfKeys.FIJI_OUTPUT_TABLE_URI)).build();

    final Fiji fiji = Fiji.Factory.open(uri, conf);
    final FijiSchemaTable schemaTable = fiji.getSchemaTable();
    try {
      final FijiTable table = fiji.openTable(uri.getTable());
      try {
        mOutputTableLayout = table.getLayout();
      } finally {
        table.release();
      }

      Preconditions.checkNotNull(mOutputTableLayout);
      mTableImportDescriptor.validateDestination(mOutputTableLayout);

      // Retrieve the classes for all of the imported columns.
      Map<FijiColumnName, Class> columnNameClassMap = Maps.newHashMap();
      for (FijiColumnName fijiColumnName
          : mTableImportDescriptor.getColumnNameSourceMap().keySet()) {
        CellSchema cellSchema = mOutputTableLayout.getCellSchema(fijiColumnName);
        switch(cellSchema.getType()) {
          case AVRO:
            // Since this is for prepackaged generic bulk importers, we can assume that we want to
            // to use the default reader schema for determining the type to write as.
            Schema.Type schemaType;
            AvroSchema as = cellSchema.getDefaultReader();
            if (as.getUid() != null) {
              Schema schema = schemaTable.getSchema(as.getUid());
              schemaType = schema.getType();
            } else if (as.getJson() != null) {
              Schema schema = new Schema.Parser().parse(as.getJson());
              schemaType = schema.getType();
            } else {
              throw new IOException("Schema is not a UID or JSON type.");
            }
            if (FIJI_AVRO_TYPE_TO_CLASS_MAP.containsKey(schemaType)) {
              columnNameClassMap.put(fijiColumnName,
                FIJI_AVRO_TYPE_TO_CLASS_MAP.get(schemaType));
            } else {
              throw new IOException("Unsupported described output type: " + cellSchema.getValue());
            }
            break;
          case INLINE:
            if (FIJI_CELL_TYPE_TO_CLASS_MAP.containsKey(cellSchema.getValue())) {
              columnNameClassMap.put(fijiColumnName,
                FIJI_CELL_TYPE_TO_CLASS_MAP.get(cellSchema.getValue()));
            } else {
              throw new IOException("Unsupported described output type: " + cellSchema.getValue());
            }
            break;
          case CLASS:
            throw new IOException("Unsupported described output type: " + cellSchema.getType());
          default:
            throw new IOException("Unsupported described output type: " + cellSchema.getType());
        }
      }
      mColumnNameClassMap = ImmutableMap.copyOf(columnNameClassMap);
    } finally {
      ResourceUtils.releaseOrLog(fiji);
    }

    setupImporter(context);
  }

  /**
   * Extensible version of {@link FijiBulkImporter#setup} for subclasses of
   * DescribedInputTextBulkImporter.
   * Does nothing by default.
   *
   * @param context A context you can use to generate EntityIds and commit writes.
   * @throws IOException on I/O error.
   */
  public void setupImporter(FijiTableContext context) throws IOException {}

  /**
   * Converts a line of text to a set of writes to <code>context</code>, and
   * an EntityId for the row.
   *
   * @param line The line to parse.
   * @param context The context to write to.
   * @throws IOException if there is an error.
   */
  public abstract void produce(Text line, FijiTableContext context)
      throws IOException;

  /**
   * Post-processes incomplete lines(Logging, keeping count, etc).
   *
   * @param line the line that was marked incomplete incomplete by the producer.
   * @param context the context in which the incompletion occured.
   * @param reason the reason why this line was incomplete.
   */
  public void incomplete(Text line, FijiTableContext context, String reason) {
    if (mIncompleteLineCounter % mLogRate == 0L) {
      LOG.error("Incomplete line: {} with reason: {}",
          line.toString(), reason);
    }
    mIncompleteLineCounter++;

    //TODO(FIJIMRLIB-9) Abort this bulk importer job early if incomplete records exceed a threshold
    context.incrementCounter(JobHistoryCounters.BULKIMPORTER_RECORDS_INCOMPLETE);

    //TODO(FIJIMRLIB-4) Add a strict mode where we reject incomplete lines
  }

  /**
   * Post-processes rejected lines(Logging, keeping count, etc).
   *
   * @param line the line that was rejected by the producer.
   * @param context the context in which the rejection occured.
   * @param reason the reason why this line was rejected.
   */
  public void reject(Text line, FijiTableContext context, String reason) {
    if (mRejectedLineCounter % mLogRate == 0L) {
      LOG.error("Rejecting line: {} with reason: {}",
          line.toString(), reason);
    }
    mRejectedLineCounter++;

    //TODO(FIJIMRLIB-9) Abort this bulk importer job early if rejected records exceed a threshold
    context.incrementCounter(JobHistoryCounters.BULKIMPORTER_RECORDS_REJECTED);

    //TODO(FIJIMRLIB-4) Allow this to emit to a rejected output so that import can be reattempted.
  }

  /**
   * Converts the value into an object of the type associated with the specified column.
   * @param fijiColumnName the destination column to infer the type from.
   * @param value string representation of the value.
   * @return object containing the parsed representation of the value.
   */
  public Object convert(FijiColumnName fijiColumnName, String value) {
    Class<?> clazz = mColumnNameClassMap.get(fijiColumnName);
    if (clazz == Boolean.class) {
      return Boolean.valueOf(value);
    } else if (clazz == Integer.class) {
      return Integer.valueOf(value);
    } else if (clazz == Long.class) {
      return Long.valueOf(value);
    } else if (clazz == Float.class) {
      return Float.valueOf(value);
    } else if (clazz == Double.class) {
      return Double.valueOf(value);
    } else if (clazz == String.class) {
      return value;
    }
    return value;
  }

  /**
   * Subclasses should implement the produce(Text line, FijiTableContext context) method instead.
   * {@inheritDoc}
   */
  @Override
  public final void produce(LongWritable fileOffset, Text line, FijiTableContext context)
      throws IOException {
    produce(line, context);
  }

  /** @return an unmodifiable collection of the columns for this bulk importer. */
  protected final Collection<FijiColumnName> getDestinationColumns() {
    Set<FijiColumnName> columns = mTableImportDescriptor.getColumnNameSourceMap().keySet();
    return Collections.unmodifiableSet(columns);
  }

  /**
   * Returns the source for the specified column, or null if the specified column is not a
   * destination column for this importer.
   *
   * @param fijiColumnName the requested Fiji column
   * @return the source for the requested column
   */
  protected final String getSource(FijiColumnName fijiColumnName) {
    return mTableImportDescriptor.getColumnNameSourceMap().get(fijiColumnName);
  }

  /** @return the source for the EntityId. */
  protected final String getEntityIdSource() {
    return mTableImportDescriptor.getEntityIdSource();
  }

  /** @return whether to override the timestamp from system time. */
  protected final boolean isOverrideTimestamp() {
    return null != mTableImportDescriptor.getOverrideTimestampSource();
  }

  /** @return the source for timestamp. */
  protected final String getTimestampSource() {
    return mTableImportDescriptor.getOverrideTimestampSource();
  }

  /**
   * Subclasses should implement the {@link #cleanupImporter(FijiTableContext)} method instead.
   * {@inheritDoc}
   */
  @Override
  public final void cleanup(FijiTableContext context) throws IOException {
    cleanupImporter(context);
  }

  /**
   * Extensible version of {@link FijiBulkImporter#cleanup} for subclasses of
   * DescribedInputTextBulkImporter.
   * Does nothing by default.
   *
   * @param context A context you can use to generate EntityIds and commit writes.
   * @throws IOException on I/O error.
   */
  public void cleanupImporter(FijiTableContext context) throws IOException {}

  /**
   * Sets the log rate - the number of lines between log statements for incomplete/rejected lines.
   *
   * @param logRateString The logging rate as a string.
   */
  @HadoopConf(key=CONF_LOG_RATE, usage="The number of lines to skip between log statements")
  protected final void setLogRate(String logRateString) {
    if (logRateString != null) {
      try {
        Long logRate = Long.parseLong(logRateString);
        mLogRate = logRate;
      } catch (NumberFormatException ne) {
        LOG.warn("Unable to parse log rate: " + logRateString);
      }
    }
  }

  /**
   * Sets the path to the text input descriptor file and parses it.
   *
   * @param inputDescriptorFile The input descriptor path.
   * @throws RuntimeException if there's an error reading or parsing the input descriptor.
   */
  @HadoopConf(key=CONF_FILE, usage="The input descriptor file.")
  protected final void setInputDescriptorPath(String inputDescriptorFile) {

    if (null == inputDescriptorFile || inputDescriptorFile.isEmpty()) {
      // Remind the user to specify this path.
      LOG.error("No input-descriptor path specified.");
      throw new RuntimeException("No input descriptor file specified on the Configuration."
          + "  Did you specify the " + CONF_FILE + " variable?");
    }

    Path descriptorPath = new Path(inputDescriptorFile);
    try {
      LOG.info("Parsing input-descriptor file: " + descriptorPath.toString());
      FileSystem fs = descriptorPath.getFileSystem(getConf());
      FSDataInputStream inputStream = fs.open(descriptorPath);
      mTableImportDescriptor =
          FijiTableImportDescriptor.createFromEffectiveJson(inputStream);

    } catch (IOException ioe) {
      LOG.error("Could not read input-descriptor file: " + descriptorPath.toString());
      throw new RuntimeException("Could not read file: " + descriptorPath.toString(), ioe);
    }
  }
}

