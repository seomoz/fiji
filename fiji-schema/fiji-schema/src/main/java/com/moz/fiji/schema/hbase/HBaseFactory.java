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

package com.moz.fiji.schema.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.annotations.Inheritance;
import com.moz.fiji.delegation.Lookups;
import com.moz.fiji.delegation.PriorityProvider;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.impl.HBaseAdminFactory;
import com.moz.fiji.schema.impl.HTableInterfaceFactory;
import com.moz.fiji.schema.layout.impl.ZooKeeperClient;
import com.moz.fiji.schema.util.LockFactory;

/** Factory for HBase instances based on URIs. */
@ApiAudience.Framework
@ApiStability.Evolving
@Inheritance.Sealed
public interface HBaseFactory extends PriorityProvider {

  /**
   * Provider for the default HBaseFactory.
   *
   * Ensures that there is only one HBaseFactory instance.
   */
  public static final class Provider {
    /** HBaseFactory instance. */
    private static final HBaseFactory INSTANCE = Lookups.getPriority(HBaseFactory.class).lookup();

    /** @return the default HBaseFactory. */
    public static HBaseFactory get() {
      return INSTANCE;
    }

    /** Utility class may not be instantiated. */
    private Provider() {
    }
  }

  /**
   * Reports a factory for HTableInterface for a given HBase instance.
   *
   * @param uri URI of the HBase instance to work with.
   * @return a factory for HTableInterface for the specified HBase instance.
   */
  HTableInterfaceFactory getHTableInterfaceFactory(FijiURI uri);

  /**
   * Reports a factory for HBaseAdmin for a given HBase instance.
   *
   * @param uri URI of the HBase instance to work with.
   * @return a factory for HBaseAdmin for the specified HBase instance.
   */
  HBaseAdminFactory getHBaseAdminFactory(FijiURI uri);

  /**
   * Gets an HConnection for the specified configuration. Caller is responsible
   * for closing this.
   *
   * @param fiji The Fiji to get a connection for.
   * @return a HConnection for the Fiji. Caller is responsible for closing this.
   */
  HConnection getHConnection(Fiji fiji);

  /**
   * Creates a lock factory for a given Fiji instance.
   *
   * @param uri URI of the Fiji instance to create a lock factory for.
   * @param conf Hadoop configuration.
   * @return a factory for locks for the specified Fiji instance.
   * @throws IOException on I/O error.
   * @deprecated {@link LockFactory} has been deprecated.
   *    Use  {@link com.moz.fiji.schema.zookeeper.ZooKeeperLock} directly.
   *    Will be removed in the future.
   */
  @Deprecated
  LockFactory getLockFactory(FijiURI uri, Configuration conf) throws IOException;

  /**
   * Creates and opens a ZooKeeperClient for a given Fiji instance.
   *
   * <p>
   *   Caller must release the ZooKeeperClient object with {@link ZooKeeperClient#release()}
   *   when done with it.
   * </p>.
   *
   * @param uri URI of the Fiji instance for which to create a ZooKeeperClient.
   * @return a new open ZooKeeperClient.
   * @throws IOException in case of an error connecting to ZooKeeper.
   * @deprecated {@link ZooKeeperClient} has been deprecated.
   *    Use {@link com.moz.fiji.schema.zookeeper.ZooKeeperUtils#getZooKeeperClient(String)} instead with
   *    the ZooKeeper ensemble from {@link FijiURI#getZooKeeperEnsemble()}.
   *    Will be removed in the future.
   */
  @Deprecated
  ZooKeeperClient getZooKeeperClient(FijiURI uri) throws IOException;

  /**
   * Returns the ZooKeeper quorum address of the provided FijiURI in comma-separated host:port
   * (standard ZooKeeper) format. This method is considered experimental and should not be called by
   * clients of Fiji Schema; instead use {@link FijiURI#getZooKeeperEnsemble()}.
   *
   * @param uri of the FijiCluster for which to return the ZooKeeper quorum address.
   * @return the ZooKeeper quorum address of the Fiji cluster.
   * @deprecated use {@link FijiURI#getZooKeeperEnsemble()} instead.
   *    Will be removed in the future.
   */
  @Deprecated
  String getZooKeeperEnsemble(FijiURI uri);
}
