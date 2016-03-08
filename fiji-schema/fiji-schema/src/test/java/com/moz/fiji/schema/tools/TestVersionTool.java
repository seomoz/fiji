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

package com.moz.fiji.schema.tools;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.util.ProtocolVersion;
import com.moz.fiji.schema.util.VersionInfo;

public class TestVersionTool extends FijiToolTest {

  @Test
  public void testVersionTool() throws Exception {
    final ProtocolVersion clientDataVersion = VersionInfo.getClientDataVersion();
    final Fiji fiji = getFiji();

    final int exitCode =
        runTool(new VersionTool(), "--debug", String.format("--fiji=%s", fiji.getURI()));

    final ProtocolVersion clusterDataVersion = fiji.getSystemTable().getDataVersion();

    assertEquals(
        "fiji client software version: " + VersionInfo.getSoftwareVersion() + "\n"
        + "fiji client data version: " + clientDataVersion + "\n"
        + "fiji cluster data version: " + clusterDataVersion + "\n"
        + "layout versions supported: "
            + FijiTableLayout.getMinSupportedLayoutVersion()
            + " to " + FijiTableLayout.getMaxSupportedLayoutVersion() + "\n",
        mToolOutputStr);

    assertEquals(0, exitCode);
  }
}
