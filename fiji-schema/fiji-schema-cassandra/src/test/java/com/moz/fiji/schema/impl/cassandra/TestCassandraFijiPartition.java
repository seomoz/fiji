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

package com.moz.fiji.schema.impl.cassandra;

import java.io.IOException;
import java.net.InetAddress;

import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Range;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.schema.cassandra.CassandraFijiClientTest;

public class TestCassandraFijiPartition extends CassandraFijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestCassandraFijiPartition.class);

  @Test
  public void testGetTokens() throws IOException {
    final CassandraFiji fiji = getFiji();
    final Session session = fiji.getCassandraAdmin().getSession();
    System.out.println(CassandraFijiPartition.getStartTokens(session));
  }

  @Test
  public void testTokenRanges() throws IOException {

    final InetAddress address1 = InetAddress.getByAddress(new byte[] {1, 1, 1, 1});
    final InetAddress address2 = InetAddress.getByAddress(new byte[] {2, 2, 2, 2});
    final InetAddress address3 = InetAddress.getByAddress(new byte[] {3, 3, 3, 3});

    Assert.assertEquals(
        ImmutableMap.of(
            Range.lessThan(-100L), address3,
            Range.closedOpen(-100L, 0L), address1,
            Range.closedOpen(0L, 100L), address2,
            Range.atLeast(100L), address3),

        CassandraFijiPartition.getTokenRanges(
            ImmutableSortedMap.of(
                -100L, address1,
                0L, address2,
                100L, address3)));
  }
}
