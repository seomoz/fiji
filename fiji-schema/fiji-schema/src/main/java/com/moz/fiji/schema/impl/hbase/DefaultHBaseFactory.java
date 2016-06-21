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
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.delegation.Priority;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiIOException;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.hbase.HBaseFactory;
import com.moz.fiji.schema.impl.HBaseAdminFactory;
import com.moz.fiji.schema.impl.HTableInterfaceFactory;
import com.moz.fiji.schema.layout.impl.ZooKeeperClient;
import com.moz.fiji.schema.util.LockFactory;

/** Factory for HBase instances based on URIs. */
@ApiAudience.Private
public final class DefaultHBaseFactory implements HBaseFactory {

  /**
   * Public constructor for use by the service loader. Clients should use
   * HBaseFactory.Provider.get(), which maintains a singleton instance.
   */
  public DefaultHBaseFactory() {
  }

  /** {@inheritDoc} */
  @Override
  public HTableInterfaceFactory getHTableInterfaceFactory(FijiURI uri) {
    return DefaultHTableInterfaceFactory.get();
  }

  /** {@inheritDoc} */
  @Override
  public HBaseAdminFactory getHBaseAdminFactory(FijiURI uri) {
    return DefaultHBaseAdminFactory.get();
  }

  /** {@inheritDoc} */
  @Override
  public HConnection getHConnection(Fiji fiji) {
    try {
      return HConnectionManager.createConnection(fiji.getConf());
    } catch (IOException ioe) {
      throw new FijiIOException("Couldn't create an HConnection.", ioe);
    }
  }

  /** {@inheritDoc} */
  @Override
  public LockFactory getLockFactory(FijiURI uri, Configuration conf) throws IOException {
    return getZooKeeperClient(uri).getLockFactory();
  }

  /** {@inheritDoc} */
  @Override
  public int getPriority(Map<String, String> runtimeHints) {
    // Default priority; should be used unless overridden by tests.
    return Priority.NORMAL;
  }

  /** {@inheritDoc} */
  @Override
  public ZooKeeperClient getZooKeeperClient(final FijiURI uri) {
    return ZooKeeperClient.getZooKeeperClient(getZooKeeperEnsemble(uri));
  }

  /** {@inheritDoc} */
  @Override
  public String getZooKeeperEnsemble(final FijiURI uri) {
    return uri.getZooKeeperEnsemble();
  }
}
