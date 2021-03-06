/**
 * (c) Copyright 2014 WibiData, Inc.
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

package com.moz.fiji.mapreduce.pivot;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.avro.dsl.JavaAvroDSL;
import com.moz.fiji.mapreduce.FijiContext;
import com.moz.fiji.mapreduce.FijiTableContext;
import com.moz.fiji.mapreduce.avro.generated.CellRewriteSpec;
import com.moz.fiji.schema.DecodedCell;
import com.moz.fiji.schema.FijiCell;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequestBuilder.ColumnsDef;
import com.moz.fiji.schema.FijiIOException;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.layout.ColumnReaderSpec;

/**
 * Pivot M/R job to rewrite cells in a Fiji table.
 *
 * <p>
 *   FijiCellRewriter is an example of a Pivot M/R job to rewrite cells in a Fiji table by
 *   performing a chain of compatible Avro conversions.
 * </p>
 *
 * <p>
 *   This job rewrites the cells from a map-type family or a fully-qualified column at a time.
 *   The rewritten cells may be written either to the same column or family of the input Fiji table,
 *   or to the same column or family of a different Fiji table, potentially in a different Fiji
 *   instance.
 * </p>
 *
 * <p>
 *   An example demonstrating how to use this job is available in {@code TestFijiCellRewriter}.
 * </p>
 */
@ApiAudience.Public
@ApiStability.Experimental
public class FijiCellRewriter extends FijiPivoter {
  private static final Logger LOG = LoggerFactory.getLogger(FijiCellRewriter.class);

  /** Configuration keys used by the cell rewriter job. */
  public static enum ConfKeys {
    /** Configuration key associated to the ColumnRewriteSpec record. */
    spec;

    /**
     * Returns the string representation of this configuration key.
     *
     * @return the string representation of this configuration key.
     */
    public String get() {
      return String.format("%s.%s", FijiCellRewriter.class.getCanonicalName(), name());
    }
  }

  /** Specific counters for this Map/Reduce job. */
  public static enum Counters {
    /** Total number of cells processed (successfully or not). */
    CELLS_PROCESSED,

    /** Total number of cells rewritten. */
    CELLS_REWRITTEN,
  }

  /** Name of the column to rewrite. */
  private FijiColumnName mColumn = null;

  /**
   * Schema rewriting rules: each datum with a schema present in this map will be rewritten.
   * For a given cell, the process is repeated until no rule applies.
   */
  private Map<Schema, Schema> mRules = null;

  /**
   * Decodes a CellRewriteSpec from an encoded entry in a Hadoop configuration.
   *
   * @param conf Hadoop configuration with a CellRewriteSpec entry.
   * @return the decoded CellRewriteSpec.
   * @throws IOException on I/O error.
   */
  private static CellRewriteSpec getSpecFromConf(final Configuration conf) throws IOException {
    final String specStr = conf.get(ConfKeys.spec.get());
    Preconditions.checkArgument(specStr != null,
        "Missing configuration entry: %s", ConfKeys.spec.get());
    final JavaAvroDSL avroDSL = new JavaAvroDSL();
    final GenericData.Record spec = avroDSL.parseValue(specStr, CellRewriteSpec.getClassSchema());
    final CellRewriteSpec specific = new CellRewriteSpec();
    for (Schema.Field field : CellRewriteSpec.getClassSchema().getFields()) {
      specific.put(field.name(), spec.get(field.name()));
    }
    return specific;
  }

  /** {@inheritDoc} */
  @Override
  public FijiDataRequest getDataRequest() {
    final CellRewriteSpec spec;
    try {
      spec = getSpecFromConf(getConf());
    } catch (IOException ioe) {
      throw new FijiIOException(ioe);
    }

    final FijiColumnName column = new FijiColumnName(spec.getColumn());

    final ColumnReaderSpec readerSpec;
    if (spec.getReaderSchema() == null) {
      readerSpec = ColumnReaderSpec.avroWriterSchemaGeneric();
    } else {
      final Schema readerSchema = new Schema.Parser().parse(spec.getReaderSchema());
      readerSpec = ColumnReaderSpec.avroReaderSchemaGeneric(readerSchema);
    }

    return FijiDataRequest.builder()
        .addColumns(ColumnsDef.create()
            .withMaxVersions(HConstants.ALL_VERSIONS)
            .add(column, readerSpec))
        .build();
  }

  /** {@inheritDoc} */
  @Override
  public void setup(FijiContext context) throws IOException {
    super.setup(context);
    final CellRewriteSpec spec = getSpecFromConf(getConf());
    mColumn = new FijiColumnName(spec.getColumn());
    LOG.info("Rewriting cells for column {}", mColumn);

    // Build the map of schema-rewrite rules:
    mRules = Maps.newHashMap();
    final JavaAvroDSL avroDSL = new JavaAvroDSL();
    for (Map.Entry<String, String> entry : spec.getRules().entrySet()) {
      final Schema fromSchema = new Schema.Parser().parse(entry.getKey());
      final Schema toSchema = new Schema.Parser().parse(entry.getValue());
      mRules.put(fromSchema, toSchema);
      LOG.info("Rewriting cell with schema {} into schema {}",
          avroDSL.schemaToString(fromSchema),
          avroDSL.schemaToString(toSchema));
    }
    // TODO(FIJIMR-264) Validate the requested conversion. In particular, detect cycles.
  }

  /** {@inheritDoc} */
  @Override
  public void produce(final FijiRowData row, final FijiTableContext context) throws IOException {
    final Iterable<FijiCell<Object>> cells;
    if (mColumn.isFullyQualified()) {
      cells = row.asIterable(mColumn.getFamily(), mColumn.getQualifier());
    } else {
      cells = row.asIterable(mColumn.getFamily());
    }

    for (FijiCell<Object> cell : cells) {
      context.incrementCounter(Counters.CELLS_PROCESSED);

      final DecodedCell<Object> original =
          new DecodedCell<Object>(cell.getWriterSchema(), cell.getData());
      final DecodedCell<Object> rewritten = rewriteCell(original);
      if (rewritten != original) {
        context.put(
            row.getEntityId(),
            mColumn.getFamily(),
            mColumn.getQualifier(),
            cell.getTimestamp(),
            rewritten.getData());
        context.incrementCounter(Counters.CELLS_REWRITTEN);
      }
    }
  }

  /**
   * Rewrites a cell.
   *
   * <p>
   *   This method is meant to be overloaded in case custom cell rewriting rules are necessary.
   * </p>
   *
   * @param cell Original value of the cell.
   * @return the new rewritten value of the cell,
   *     or the original cell value if no translation rules apply.
   * @throws IOException on I/O error.
   *
   * @param <U> type of the input cell to rewrite.
   * @param <T> type to rewrite the input cell into.
   */
  protected <T, U> DecodedCell<T> rewriteCell(final DecodedCell<U> cell) throws IOException {
    // Apply conversion rules as long as some rule matches:
    DecodedCell<U> rewritten = cell;
    while (true) {
      final Schema newSchema = mRules.get(rewritten.getWriterSchema());
      if (newSchema == null) {
        // No rule apply, we are done.
        break;
      } else {
        rewritten = convertAvro(rewritten, newSchema);
      }
    }
    return (DecodedCell<T>) rewritten;
  }

  /**
   * Converts an Avro datum using a new (compatible) Avro schema.
   *
   * @param original Original Avro datum with its schema.
   * @param schema New Avro schema to convert the datum into.
   * @return the Avro datum converted into the specified new schema.
   * @throws IOException on I/O error.
   *
   * @param <U> type of the input cell to rewrite.
   * @param <T> type to rewrite the input cell into.
   */
  public static <T, U> DecodedCell<T> convertAvro(
      DecodedCell<U> original,
      Schema schema
  ) throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    // Encode original datum to bytes:
    final Encoder encoder = EncoderFactory.get().directBinaryEncoder(baos, null);
    final DatumWriter<U> writer = new GenericDatumWriter<U>(original.getWriterSchema());
    writer.write(original.getData(), encoder);
    encoder.flush();

    // Decode bytes according to the new schema:
    final Decoder decoder = DecoderFactory.get().binaryDecoder(baos.toByteArray(), null);
    final DatumReader<T> reader =
        new GenericDatumReader<T>(original.getWriterSchema(), schema);
    final T data = reader.read(null, decoder);

    return new DecodedCell<T>(schema, data);
  }
}
