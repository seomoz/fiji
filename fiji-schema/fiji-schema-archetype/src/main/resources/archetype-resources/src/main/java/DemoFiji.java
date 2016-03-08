#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
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

package ${package};

import java.io.FileInputStream;
import java.io.IOException;

import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.FijiTableWriter;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.layout.FijiTableLayout;

/**
 * A demonstration of the Fiji API.
 *
 * Here, we assume the table description is described by json,
 * it is in table_desc.json.
 */
public final class DemoFiji {
  /**
   * Private constructor for utility class.
   */
  private DemoFiji() {
    // No-op private constructor. This class's main method
    // should be run from the command line.
  }

  /**
   * Main method (run from command line).
   *
   * @param args Command line arguments.
   * @throws IOException If opening the fiji fails.
   */
  public static void main(String[] args) throws IOException {
    // First we define some constants that we use for our demo.

    // The table description is a JSON file that we read in.
    final String tableDesc = "src/main/layout/table_desc.json";
    // The name of the table we will create in this demo.
    final String tableName = "users";
    // The ID of the entity we will use in this demo.
    final String userId = "123";
    // The name of the user we are putting into our table.
    final String username = "foo";

    // Fiji instances are specified by FijiURIs, formatted as below.
    // This is the default fiji instance.
    final String uri = "fiji://.env/default";
    final FijiURI fijiURI = FijiURI.newBuilder(uri).build();

    // Open the fiji specified.
    Fiji fiji = Fiji.Factory.open(fijiURI);

    // Always surround with a try {} finally{} so the fiji gets released,
    // no matter what happens.
    try {
      // ----- Create a fiji table. -----
      // First, we need to create a table layout.
      final FijiTableLayout layout =
        FijiTableLayout.newLayout(
          FijiTableLayout.readTableLayoutDescFromJSON(
              new FileInputStream(tableDesc)));

      // Create the fiji table.
      fiji.createTable(tableName, layout);
      // Get a handle to the table.
      FijiTable table = fiji.openTable(tableName);

      // Get the entity ID, according to this table, of the user we are
      // demonstrating with.
      EntityId entityId = table.getEntityId(userId);

      // ----- Write a row to the table. -----
      // Get a TableWriter for our table.
      FijiTableWriter tableWriter = table.openTableWriter();
      // Surround with a try/finally so the tablewriter gets closed.
      try {
        System.out.println("Putting user " + username + " into table.");
        tableWriter.put(entityId, "info", "name", username);
        // Flush the write to the table, since this is a demo and
        // we are not concerned about efficiency, we just want to
        // show that the cell got written successfully.
        tableWriter.flush();
      } finally {
        tableWriter.close();
      }

      // ----- Read a row from the table. -----
      // Get a TableReader for our table.
      FijiTableReader tableReader = table.openTableReader();
      // Surround with a try/finally so the tablereader gets closed.
      try {
        // Build a DataRequest for the row we want.
        FijiDataRequest dataRequest = FijiDataRequest.create("info", "name");
        FijiRowData result = tableReader.get(entityId, dataRequest);
        String name = result.getMostRecentValue("info", "name").toString();
        System.out.println("Read username " + name + " from table.");
      } finally {
        tableReader.close();
      }
    } finally {
      fiji.release();
    }
  }
}
