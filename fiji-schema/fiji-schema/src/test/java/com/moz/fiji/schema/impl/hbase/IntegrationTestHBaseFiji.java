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
package com.moz.fiji.schema.impl.hbase;

import java.io.IOException;

import junit.framework.Assert;
import org.junit.Test;

import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.avro.TableLayoutDesc;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.testutil.AbstractFijiIntegrationTest;
import com.moz.fiji.schema.util.ProtocolVersion;

/** Tests for HBaseFiji. */
public class IntegrationTestHBaseFiji extends AbstractFijiIntegrationTest {
  /** Validates the constructor of HBaseFiji for data version "system-2.0". */
  @Test
  public void testHBaseFijiSystem2dot0() throws Exception {
    final FijiURI uri = getFijiURI();
    setDataVersion(uri, ProtocolVersion.parse("system-2.0"));
    final HBaseFiji fiji = (HBaseFiji) Fiji.Factory.open(uri);
    try {
      Assert.assertNotNull(fiji.getZKClient());
    } finally {
      fiji.release();
    }
  }

  /** Validates HBaseFiji.modifyTableLayout() for data version "system-2.0". */
  @Test
  public void testHBaseFijiSystemModifyTableLayout2dot0() throws Exception {
    final FijiURI uri = getFijiURI();
    setDataVersion(uri, ProtocolVersion.parse("system-2.0"));
    final HBaseFiji fiji = (HBaseFiji) Fiji.Factory.open(uri);
    try {
      Assert.assertNotNull(fiji.getZKClient());

      fiji.createTable(FijiTableLayouts.getLayout(FijiTableLayouts.FOO_TEST));
      final FijiTable table = fiji.openTable("foo");
      try {
        final TableLayoutDesc layoutUpdate =
            TableLayoutDesc.newBuilder(table.getLayout().getDesc()).build();
        layoutUpdate.setReferenceLayout(layoutUpdate.getLayoutId());
        layoutUpdate.setLayoutId("2");

        final FijiTableLayout newLayout = fiji.modifyTableLayout(layoutUpdate);

      } finally {
        table.release();
      }

    } finally {
      fiji.release();
    }
  }


  /**
   * Sets the data version of a Fiji instance.
   *
   * @param uri URI of the Fiji instance to configure the data version of.
   * @param version Data version to use.
   * @throws IOException on I/O error.
   */
  private static void setDataVersion(final FijiURI uri, final ProtocolVersion version)
      throws IOException {
    final Fiji fiji = Fiji.Factory.open(uri);
    try {
      fiji.getSystemTable().setDataVersion(version);
    } finally {
      fiji.release();
    }
  }
}
