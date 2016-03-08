/**
 * (c) Copyright 2015 WibiData, Inc.
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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedBytes;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.layout.FijiTableLayouts;

public class TestHBaseFijiPartition extends FijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestHBaseFijiPartition.class);

  @Test
  public void testGetSinglePartition() throws IOException {
    final HBaseFiji fiji = (HBaseFiji) getFiji();
    fiji.createTable(FijiTableLayouts.getLayout(FijiTableLayouts.SIMPLE));
    final HBaseFijiTable table = fiji.openTable("table");
    final Collection<HBaseFijiPartition> partitions = table.getPartitions();

    Assert.assertTrue(partitions.size() == 1);
    final HBaseFijiPartition partition = partitions.iterator().next();
    Assert.assertArrayEquals(partition.getStartKey(), new byte[] {});
    Assert.assertArrayEquals(partition.getEndKey(), new byte[] {});
  }

  @Test
  public void testGetMultiplePartitions() throws IOException {
    final HBaseFiji fiji = (HBaseFiji) getFiji();
    fiji.createTable(
        FijiTableLayouts.getLayout(FijiTableLayouts.SIMPLE),
        new byte[][]{
            new byte[]{0x01},
            new byte[]{0x02},
        });
    final HBaseFijiTable table = fiji.openTable("table");
    final List<HBaseFijiPartition> partitions = Lists.newArrayList(table.getPartitions());
    Collections.sort(partitions, new HBaseFijiPartitionComparator());

    final HBaseFijiPartition p1 = partitions.get(0);
    Assert.assertArrayEquals(p1.getStartKey(), new byte[] {});
    Assert.assertArrayEquals(p1.getEndKey(), new byte[] {0x01});

    final HBaseFijiPartition p2 = partitions.get(1);
    Assert.assertArrayEquals(p2.getStartKey(), new byte[] {0x01});
    Assert.assertArrayEquals(p2.getEndKey(), new byte[] {0x02});

    final HBaseFijiPartition p3 = partitions.get(2);
    Assert.assertArrayEquals(p3.getStartKey(), new byte[] {0x02});
    Assert.assertArrayEquals(p3.getEndKey(), new byte[] {});
  }

  /**
   * Compare HBase Fiji Partitions by the start key.
   */
  private static final class HBaseFijiPartitionComparator
      implements Comparator<HBaseFijiPartition> {

    /** {@inheritDoc} */
    @Override
    public int compare(
        final HBaseFijiPartition p1,
        final HBaseFijiPartition p2
    ) {
      return UnsignedBytes.lexicographicalComparator().compare(p1.getStartKey(), p2.getStartKey());
    }
  }
}
