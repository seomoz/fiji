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

package com.moz.fiji.examples.phonebook;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Ignore;
import org.junit.Test;

import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.avro.TableLayoutDesc;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.shell.api.Client;
import com.moz.fiji.schema.util.Resources;

/**
 * Test that DDL and JSON layouts are correct for this version of FijiSchema.
 */
public class TestLayouts extends FijiClientTest {
  /**
   * Test that DDL command creates the table correctly.
   *
   * @throws IOException if there's a problem interacting w/ FijiURI, etc.
   */
  @Test
  public void testDDLCreate() throws IOException {
    FijiURI uri = getFiji().getURI();
    Client ddlClient = Client.newInstance(uri);
    ddlClient.executeStream(Resources.openSystemResource("phonebook/layout.ddl"));
    System.out.println(ddlClient.getLastOutput());
    ddlClient.close();
  }

  /**
   * Test that the JSON file can be used with fiji create-table.
   *
   * @throws IOException if there's a problem parsing the JSON file or creating the table.
   */
  @Test
  public void testJSONFileIsValid() throws IOException {
    TableLayoutDesc layout = FijiTableLayouts.getLayout("phonebook/layout.json");
    getFiji().createTable(layout);
  }

  // TODO(WDPHONE-9): Fix this test.
  /**
   * Test that the JSON file and the DDL create identical tables.
   *
   * @throws IOException if things go wrong with either side of this.
   */
  @Test
  @Ignore
  public void testDDLMatchesJSON() throws IOException {
    // Create an instance through the DDL.
    FijiURI uri = getFiji().getURI();
    Client ddlClient = Client.newInstance(uri);
    ddlClient.executeStream(Resources.openSystemResource("phonebook/layout.ddl"));
    System.out.println(ddlClient.getLastOutput());

    FijiTableLayout ddlLayout = getFiji().getMetaTable().getTableLayout("phonebook");
    final String realJsonFromDDL = ddlLayout.getDesc().toString();

    // Now delete the instance we just created...
    ddlClient.executeUpdate("DROP TABLE phonebook");
    ddlClient.close();

    // ... So we can create a new one via the JSON file

    TableLayoutDesc layout = FijiTableLayouts.getLayout("phonebook/layout.json");
    getFiji().createTable(layout);

    // Test that the two have equivalent representations.

    FijiTableLayout retrievedFromJson = getFiji().getMetaTable().getTableLayout("phonebook");
    final String realJsonFromJson = retrievedFromJson.getDesc().toString();

    if (!ddlLayout.equals(retrievedFromJson)) {
      System.out.println("Real JSON for the real layout from the DDL doesn't match the file");
      System.out.println("JSON layout retrieved from DDL statement:");
      System.out.println(realJsonFromDDL);
      System.out.println("JSON layout parsed by reading in layout.json:");
      System.out.println(realJsonFromJson);
      assertEquals("JSON is not equal", realJsonFromDDL, realJsonFromJson);
    }
  }
}
