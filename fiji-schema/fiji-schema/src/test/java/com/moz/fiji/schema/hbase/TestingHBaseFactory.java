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
import java.util.Map;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.delegation.Priority;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.impl.HBaseAdminFactory;
import com.moz.fiji.schema.impl.HTableInterfaceFactory;
import com.moz.fiji.schema.impl.hbase.DefaultHBaseFactory;
import com.moz.fiji.schema.layout.impl.ZooKeeperClient;
import com.moz.fiji.schema.util.LockFactory;
import com.moz.fiji.schema.zookeeper.TestingZooKeeperFactory;
import com.moz.fiji.testing.fakehtable.FakeHBase;

/** Factory for HBase instances based on URIs. */
public final class TestingHBaseFactory implements HBaseFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TestingHBaseFactory.class);

  /** Factory to delegate to. */
  private static final HBaseFactory DELEGATE = new DefaultHBaseFactory();

  /** Map from fake HBase ID to fake HBase instances. */
  private final LoadingCache<String, FakeHBase> mFakeHBases = CacheBuilder.newBuilder()
      .build(
          new CacheLoader<String, FakeHBase>() {
            @Override
            public FakeHBase load(String fakeID) {
              return new FakeHBase();
            }
          });

  /**
   * Public constructor. This should not be directly invoked by users; you should
   * use HBaseFactory.get(), which retains a singleton instance.
   */
  public TestingHBaseFactory() {
  }

  /**
   * Gets the FakeHBase for a given HBase URI.
   *
   * @param uri URI of a fake HBase instance.
   * @return the FakeHBase for the specified URI, or null if the URI does not specify a fake HBase.
   */
  public FakeHBase getFakeHBase(FijiURI uri) {
    final String fakeID = TestingZooKeeperFactory.getFakeClusterID(uri);
    if (fakeID == null) {
      return null;
    }
    return mFakeHBases.getUnchecked(fakeID);
  }

  /** {@inheritDoc} */
  @Override
  public HTableInterfaceFactory getHTableInterfaceFactory(FijiURI uri) {
    final FakeHBase fake = getFakeHBase(uri);
    if (fake != null) {
      return fake.getHTableFactory();
    }
    return DELEGATE.getHTableInterfaceFactory(uri);
  }

  /** {@inheritDoc} */
  @Override
  public HBaseAdminFactory getHBaseAdminFactory(FijiURI uri) {
    final FakeHBase fake = getFakeHBase(uri);
    if (fake != null) {
      return fake.getAdminFactory();
    }
    return DELEGATE.getHBaseAdminFactory(uri);
  }

  /** {@inheritDoc} */
  @Override
  public HConnection getHConnection(Fiji fiji) {
    final FakeHBase fake = getFakeHBase(fiji.getURI());
    if (fake != null) {
      return fake.getHConnection();
    }
    return DELEGATE.getHConnection(fiji);
  }

  /** {@inheritDoc} */
  @Override
  public LockFactory getLockFactory(FijiURI uri, Configuration conf) throws IOException {
    return DELEGATE.getLockFactory(uri, conf);
  }

  /** {@inheritDoc} */
  @Override
  public int getPriority(Map<String, String> runtimeHints) {
    // Higher priority than default factory.
    return Priority.HIGH;
  }

  /** {@inheritDoc} */
  @Override
  public ZooKeeperClient getZooKeeperClient(FijiURI uri) throws IOException {
    return DELEGATE.getZooKeeperClient(uri);
  }

  /** {@inheritDoc} */
  @Override
  public String getZooKeeperEnsemble(FijiURI uri) {
    return uri.getZooKeeperEnsemble();
  }
}
