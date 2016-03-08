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
import java.util.Iterator;

import com.google.common.base.Preconditions;

import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequest.Column;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiRowDataTest;
import com.moz.fiji.schema.FijiRowScanner;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.FijiTableReader.FijiScannerOptions;
import com.moz.fiji.schema.layout.FijiTableLayout.LocalityGroupLayout.FamilyLayout;

/**
 * Test of {@link FijiRowData} for HBase Fiji scans.
 */
public class TestHBaseScanFijiRowData extends FijiRowDataTest {

  /** {@inheritDoc} */
  @Override
  public FijiRowData getRowData(
      final FijiTable table,
      final FijiTableReader reader,
      final EntityId eid,
      final FijiDataRequest dataRequest
  ) throws IOException {
    final FijiScannerOptions options = new FijiScannerOptions();
    options.setStartRow(eid);
    final FijiRowScanner scanner = reader.getScanner(dataRequest, options);
    try {
      final Iterator<FijiRowData> itr = scanner.iterator();
      if (itr.hasNext()) {
        final FijiRowData scanData = itr.next();
        if (scanData.getEntityId().equals(eid)) {
          return scanData;
        }
      }
      // Fall back to normal get (should be empty);
      final FijiRowData getData = reader.get(eid, dataRequest);

      for (final Column columnRequest : dataRequest.getColumns()) {
        if (!columnRequest.isPagingEnabled()) {
          final FijiColumnName column = columnRequest.getColumnName();

          if (column.isFullyQualified()) {
            Preconditions.checkState(
                getData.getCells(column.getFamily(), column.getQualifier()).isEmpty(),
                "Fell back to a get of a non-empty row.");
          } else {
            final FamilyLayout family = table.getLayout().getFamilyMap().get(column.getFamily());

            if (family.isMapType()) {
              Preconditions.checkState(getData.getCells(columnRequest.getFamily()).isEmpty(),
                  "Fell back to a get of a non-empty row.");
            } else {
              for (final String qualifier : family.getColumnMap().keySet()) {
                Preconditions.checkState(
                    getData.getCells(columnRequest.getFamily(), qualifier).isEmpty(),
                    "Fell back to a get of a non-empty row.");
              }
            }
          }
        }
      }
      return getData;
    } finally {
      scanner.close();
    }
  }
}
