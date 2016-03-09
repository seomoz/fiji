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

package com.moz.fiji.hive;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.impl.hbase.HBaseFijiTable;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.util.ResourceUtils;

/**
 * Contains all the information about a fiji table relevant to using hive.
 */
public class FijiTableInfo implements Closeable {
  public static final String FIJI_TABLE_URI = "fiji.table.uri";

  private final FijiURI mFijiURI;

  private Connection mConnection;

  /**
   * Constructor.
   *
   * @param properties The Hive table properties.
   */
  public FijiTableInfo(Properties properties) {
    mFijiURI = getURIFromProperties(properties);
    mConnection = null;
  }

  /**
   * Gets the URI from the passed in properties.
   * @param properties for the job.
   * @return FijiURI extracted from the passed in properties.
   */
  public static FijiURI getURIFromProperties(Properties properties) {
    String fijiURIString = properties.getProperty(FIJI_TABLE_URI);
    //TODO Pretty exceptions for URI parser issues.
    FijiURI fijiURI = FijiURI.newBuilder(fijiURIString).build();

    //TODO Ensure that this URI has a table component.
    return fijiURI;
  }

  /**
   * Gets the URI for this FijiTableInfo.
   *
   * @return FijiURI associated with this FijiTableInfo
   */
  public FijiURI getFijiTableURI() {
    return mFijiURI;
  }

  /**
   * Gets the Fiji table layout.
   *
   * @return The table layout.
   * @throws IOException If it cannot be read.
   */
  public FijiTableLayout getTableLayout() throws IOException {
    return getConnection().getTableLayout();
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    IOUtils.closeQuietly(mConnection);
  }

  /**
   * Gets a connection to the Fiji table.
   *
   * @return A connection.
   * @throws IOException If the connection cannot be established.
   */
  private Connection getConnection() throws IOException {
    if (null == mConnection) {
      mConnection = new Connection(mFijiURI);
    }
    return mConnection;
  }

  /**
   * Connection to a Fiji table and its metadata.
   */
  private static class Connection implements Closeable {
    /** The Fiji connection. */
    private final Fiji mFiji;
    /** The Fiji table connection. */
    private final FijiTable mFijiTable;

    /**
     * Opens a connection.
     * @param fijiURI The fijiURI
     * @throws IOException If there is a connection error.
     */
    public Connection(FijiURI fijiURI)
        throws IOException {
      final Configuration conf = HBaseConfiguration.create();

      mFiji = Fiji.Factory.open(fijiURI);
      mFijiTable = mFiji.openTable(fijiURI.getTable());
    }

    /**
     * Gets the layout of the Fiji table.
     *
     * @return The table layout.
     */
    public FijiTableLayout getTableLayout() {
      return HBaseFijiTable.downcast(mFijiTable).getLayout();
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      ResourceUtils.releaseOrLog(mFijiTable);
      ResourceUtils.releaseOrLog(mFiji);
    }
  }
}
