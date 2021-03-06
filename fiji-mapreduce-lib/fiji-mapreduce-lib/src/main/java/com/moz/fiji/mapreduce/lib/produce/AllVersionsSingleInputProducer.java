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

package com.moz.fiji.mapreduce.lib.produce;

import java.util.Collection;

import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequestBuilder;

/**
 * Base class for producers that read all versions from a single input column.
 */
public abstract class AllVersionsSingleInputProducer extends SingleInputProducer {
  @Override
  public FijiDataRequest getDataRequest() {
    FijiDataRequest request = super.getDataRequest();
    Collection<FijiDataRequest.Column> columns = request.getColumns();
    if (columns.size() != 1) {
      throw new RuntimeException("Should be exactly one input column");
    }
    FijiDataRequest.Column col = columns.iterator().next();

    FijiDataRequestBuilder out = FijiDataRequest.builder();
    out.withTimeRange(request.getMinTimestamp(), request.getMaxTimestamp())
        .newColumnsDef().withMaxVersions(Integer.MAX_VALUE)
            .withPageSize(col.getPageSize())
            .withFilter(col.getFilter())
            .add(col.getFamily(), col.getQualifier());
    return out.build();
  }
}
