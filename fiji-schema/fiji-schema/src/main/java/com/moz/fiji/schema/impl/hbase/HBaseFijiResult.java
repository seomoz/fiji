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

package com.moz.fiji.schema.impl.hbase;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.hbase.client.Result;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequest.Column;
import com.moz.fiji.schema.FijiDataRequestBuilder;
import com.moz.fiji.schema.FijiResult;
import com.moz.fiji.schema.impl.DefaultFijiResult;
import com.moz.fiji.schema.impl.EmptyFijiResult;
import com.moz.fiji.schema.layout.HBaseColumnNameTranslator;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.impl.CellDecoderProvider;

/**
 * A utility class which can create a {@link FijiResult} view on an HBase Fiji table.
 */
@ApiAudience.Private
public final class HBaseFijiResult {

  /**
   * Create a new {@link FijiResult} backed by HBase.
   *
   * @param entityId EntityId of the row from which to read cells.
   * @param dataRequest FijiDataRequest defining the values to retrieve.
   * @param unpagedRawResult The unpaged results from the row.
   * @param table The table being viewed.
   * @param layout The layout of the table.
   * @param columnTranslator A column name translator for the table.
   * @param decoderProvider A cell decoder provider for the table.
   * @param <T> The type of value in the {@code FijiCell} of this view.
   * @return an {@code HBaseFijiResult}.
   * @throws IOException if error while decoding cells.
   */
  public static <T> FijiResult<T> create(
      final EntityId entityId,
      final FijiDataRequest dataRequest,
      final Result unpagedRawResult,
      final HBaseFijiTable table,
      final FijiTableLayout layout,
      final HBaseColumnNameTranslator columnTranslator,
      final CellDecoderProvider decoderProvider
  ) throws IOException {
    final Collection<Column> columnRequests = dataRequest.getColumns();
    final FijiDataRequestBuilder unpagedRequestBuilder = FijiDataRequest.builder();
    final FijiDataRequestBuilder pagedRequestBuilder = FijiDataRequest.builder();
    unpagedRequestBuilder.withTimeRange(
        dataRequest.getMinTimestamp(),
        dataRequest.getMaxTimestamp());
    pagedRequestBuilder.withTimeRange(dataRequest.getMinTimestamp(), dataRequest.getMaxTimestamp());

    for (Column columnRequest : columnRequests) {
      if (columnRequest.isPagingEnabled()) {
        pagedRequestBuilder.newColumnsDef(columnRequest);
      } else {
        unpagedRequestBuilder.newColumnsDef(columnRequest);
      }
    }

    final CellDecoderProvider requestDecoderProvider =
        decoderProvider.getDecoderProviderForRequest(dataRequest);

    final FijiDataRequest unpagedRequest = unpagedRequestBuilder.build();
    final FijiDataRequest pagedRequest = pagedRequestBuilder.build();

    if (unpagedRequest.isEmpty() && pagedRequest.isEmpty()) {
      return new EmptyFijiResult<T>(entityId, dataRequest);
    }

    final HBaseMaterializedFijiResult<T> materializedFijiResult;
    if (!unpagedRequest.isEmpty()) {
      materializedFijiResult =
          HBaseMaterializedFijiResult.create(
              entityId,
              unpagedRequest,
              unpagedRawResult,
              layout,
              columnTranslator,
              requestDecoderProvider);
    } else {
      materializedFijiResult = null;
    }

    final HBasePagedFijiResult<T> pagedFijiResult;
    if (!pagedRequest.isEmpty()) {
      pagedFijiResult =
          new HBasePagedFijiResult<T>(
              entityId,
              pagedRequest,
              table,
              layout,
              columnTranslator,
              requestDecoderProvider);
    } else {
      pagedFijiResult = null;
    }

    if (unpagedRequest.isEmpty()) {
      return pagedFijiResult;
    } else if (pagedRequest.isEmpty()) {
      return materializedFijiResult;
    } else {
      return DefaultFijiResult.create(dataRequest, materializedFijiResult, pagedFijiResult);
    }
  }

  /**
   * Constructor for non-instantiable helper class.
   */
  private HBaseFijiResult() { }

}
