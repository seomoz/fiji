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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.avro.TableLayoutDesc;
import com.moz.fiji.schema.layout.InvalidLayoutException;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.util.ToJson;

public class TestLayoutTool extends FijiToolTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestLayoutTool.class);

  /**
   * Writes a table layout as a JSON descriptor in a temporary file.
   *
   * @param layoutDesc Table layout descriptor to write.
   * @return the temporary File where the layout has been written.
   * @throws Exception on error.
   */
  private File getTempLayoutFile(TableLayoutDesc layoutDesc) throws Exception {
    final File layoutFile = File.createTempFile(layoutDesc.getName(), ".json", getLocalTempDir());
    final OutputStream fos = new FileOutputStream(layoutFile);
    try {
      IOUtils.write(ToJson.toJsonString(layoutDesc), fos);
    } finally {
      fos.close();
    }
    return layoutFile;
  }

  @Test
  public void testChangeRowKeyHashing() throws Exception {
    final FijiTableLayout layout =
            FijiTableLayouts.getTableLayout(FijiTableLayouts.FOO_TEST_LEGACY);
    getFiji().createTable(layout.getDesc());

    final File newLayoutFile = getTempLayoutFile(FijiTableLayouts.getFooChangeHashingTestLayout());
    final FijiURI tableURI =
        FijiURI.newBuilder(getFiji().getURI()).withTableName(layout.getName()).build();

    try {
      runTool(new LayoutTool(),
        "--table=" + tableURI,
        "--do=set",
        "--layout=" + newLayoutFile
        );
      fail("Should throw InvalidLayoutException");
    } catch (InvalidLayoutException ile) {
      assertTrue(ile.getMessage().startsWith(
          "Invalid layout update from reference row keys format"));
    }
  }
}
