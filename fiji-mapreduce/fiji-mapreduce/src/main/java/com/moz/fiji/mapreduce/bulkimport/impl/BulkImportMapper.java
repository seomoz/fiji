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

package com.moz.fiji.mapreduce.bulkimport.impl;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.mapreduce.FijiMapper;
import com.moz.fiji.mapreduce.FijiTableContext;
import com.moz.fiji.mapreduce.avro.AvroKeyReader;
import com.moz.fiji.mapreduce.avro.AvroValueReader;
import com.moz.fiji.mapreduce.bulkimport.FijiBulkImporter;
import com.moz.fiji.mapreduce.framework.HFileKeyValue;
import com.moz.fiji.mapreduce.framework.JobHistoryCounters;
import com.moz.fiji.mapreduce.impl.HTableReader;
import com.moz.fiji.mapreduce.impl.FijiTableContextFactory;

/**
 * Hadoop mapper that runs a FijiBulkImporter.
 *
 * <p>The input to this mapper depends on the format of the data being imported.  For
 * example, if you were importing data from text files using a TextInputFormat, the
 * key would be a LongWritable byte file offset, and the value would be a Text containing
 * a line from the file.</p>
 *
 * <p>The task of generating ProducedWrites that represent data to be imported into a Fiji table
 * is delegated to a FijiBulkImporter subclass whose classname is set in the
 * Configuration.  This mapper will forward each input record to the FijiBulkImporter to
 * allow it to produce the ProducedWrites.  The obtained ProducedWrites will be used to
 * generate an appropriate HBase Put object (output as the map output value).  The map
 * output key is the row of the HTable the Put should be applied to.</p>
 *
 * <p>When paired with a PutSortReducer and an HFileOutputFormat, HFiles will be generated
 * that are ready to be loaded directly into the HBase HTables used for the backing store
 * of a Fiji table.  When run as a map-only job with an HBase TableOutputFormat, the Puts
 * will be sent directly to the HTable for committing.</p>
 *
 * @param <K> Type of the MapReduce input key.
 * @param <V> Type of the MapReduce input value.
 */
@ApiAudience.Private
public final class BulkImportMapper<K, V>
    extends FijiMapper<K, V, HFileKeyValue, NullWritable>
    implements Configurable, AvroKeyReader, AvroValueReader, HTableReader {

  private static final Logger LOG = LoggerFactory.getLogger(BulkImportMapper.class);

  /** The job configuration. */
  private Configuration mConf;

  /** The FijiBulkImporter instance to delegate the import work to. */
  private FijiBulkImporter<K, V> mBulkImporter;

  /** Fiji context for bulk-importers. */
  private FijiTableContext mTableContext;

  /** {@inheritDoc} */
  @Override
  public void setConf(Configuration conf) {
    mConf = conf;
  }

  /** {@inheritDoc} */
  @Override
  public Configuration getConf() {
    return mConf;
  }

  /** {@inheritDoc} */
  @Override
  protected void setup(Context context) throws IOException {
    try {
      super.setup(context);
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }

    final Configuration conf = context.getConfiguration();
    mBulkImporter = FijiBulkImporters.create(conf);
    mTableContext = FijiTableContextFactory.create(context);

    mBulkImporter.setup(mTableContext);
  }

  @Override
  protected void map(K key, V value, Context mapContext)
      throws IOException {
    mBulkImporter.produce(key, value, mTableContext);
    mapContext.getCounter(JobHistoryCounters.BULKIMPORTER_RECORDS_PROCESSED).increment(1);
  }

  /** {@inheritDoc} */
  @Override
  protected void cleanup(Context context) throws IOException {
    Preconditions.checkNotNull(mTableContext);
    mBulkImporter.cleanup(mTableContext);
    mTableContext.close();
    mTableContext = null;
    try {
      super.cleanup(context);
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroKeyReaderSchema() throws IOException {
    FijiBulkImporter<K, V> bulkImporter = FijiBulkImporters.create(getConf());
    if (bulkImporter instanceof AvroKeyReader) {
      LOG.debug("Bulk importer " + bulkImporter.getClass().getName()
          + " implements AvroKeyReader, querying for reader schema.");
      return ((AvroKeyReader) bulkImporter).getAvroKeyReaderSchema();
    }
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroValueReaderSchema() throws IOException {
    FijiBulkImporter<K, V> bulkImporter = FijiBulkImporters.create(getConf());
    if (bulkImporter instanceof AvroValueReader) {
      LOG.debug("Bulk importer " + bulkImporter.getClass().getName()
          + " implements AvroValueReader, querying for reader schema.");
      return ((AvroValueReader) bulkImporter).getAvroValueReaderSchema();
    }
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public Scan getInputHTableScan(Configuration conf) throws IOException {
    FijiBulkImporter<K, V> bulkImporter = FijiBulkImporters.create(conf);
    if (bulkImporter instanceof HTableReader) {
      LOG.debug("Bulk importer " + bulkImporter.getClass().getName()
          + " implements HTableReader, querying for input HTable Scan specification.");
      return ((HTableReader) bulkImporter).getInputHTableScan(conf);
    }
    // The wrapped bulk importer doesn't need to read from an HTable, so return null to
    // tell the caller that we also don't need to read from an HTable.
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputKeyClass() {
    return HFileKeyValue.class;
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return NullWritable.class;
  }
}
