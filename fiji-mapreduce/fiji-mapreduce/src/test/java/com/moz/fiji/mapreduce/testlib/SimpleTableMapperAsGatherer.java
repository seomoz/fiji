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

package com.moz.fiji.mapreduce.testlib;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.mapreduce.FijiTableContext;
import com.moz.fiji.mapreduce.framework.HFileKeyValue;
import com.moz.fiji.mapreduce.gather.GathererContext;
import com.moz.fiji.mapreduce.gather.FijiGatherer;
import com.moz.fiji.mapreduce.gather.impl.InternalGathererContext;
import com.moz.fiji.mapreduce.impl.FijiTableContextFactory;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRowData;

/**
 * Example of a «table mapper» implemented as a gatherer that output to a Fiji table.
 *
 * <p> A table mapper reads from a Fiji table and writes to another Fiji table
 *    (or possible the same).
 *
 * <p> This mapper expects an input table with the layout specified in
 *         src/test/resources/com.moz.fiji/mapreduce/layout/test.json,
 *     reads basic users info (info:first_name, info:last_name and info:zip_code),
 *     and writes rows whose ID are the zip codes.
 *     It writes "first_name last_name" at column "primitives:string".
 *
 * Most of the boilerplate could be avoided :(
 */
public class SimpleTableMapperAsGatherer
    extends FijiGatherer<HFileKeyValue, NullWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleTableMapperAsGatherer.class);

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

  /** {@inheritDoc} */
  @Override
  public FijiDataRequest getDataRequest() {
    return FijiDataRequest.create("info");
  }

  private FijiTableContext mTableContext = null;

  /** {@inheritDoc} */
  @Override
  public void setup(GathererContext<HFileKeyValue, NullWritable> context) throws IOException {
    Preconditions.checkState(mTableContext == null);
    super.setup(context);
    mTableContext =
        FijiTableContextFactory.create(((InternalGathererContext)context).getMapReduceContext());
  }

  /** {@inheritDoc} */
  @Override
  public void gather(FijiRowData input, GathererContext<HFileKeyValue, NullWritable> unused)
      throws IOException {
    Preconditions.checkState(mTableContext != null);

    final String firstName = input.getMostRecentValue("info", "first_name").toString();
    final String lastName = input.getMostRecentValue("info", "last_name").toString();
    final Integer zipCode = input.getMostRecentValue("info", "zip_code");
    LOG.info(String.format("Processing row: %s %s %d", firstName, lastName, zipCode));
    // Note: this is actually dangerous,
    //     to accumulating several persons with the same zip-code,
    //     we must ensure different timestamps.
    mTableContext.put(
        mTableContext.getEntityId(zipCode.toString()),
        "primitives", "string", System.currentTimeMillis(),
        String.format("%s %s", firstName, lastName));
    LOG.info(String.format("Processed row: %s %s %d", firstName, lastName, zipCode));
  }

  /** {@inheritDoc} */
  @Override
  public void cleanup(GathererContext<HFileKeyValue, NullWritable> context) throws IOException {
    Preconditions.checkState(mTableContext != null);
    mTableContext.close();
    mTableContext = null;
    super.cleanup(context);
  }
}
