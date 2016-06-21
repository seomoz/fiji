/**
 * (c) Copyright 2014 WibiData, Inc.
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

package com.moz.fiji.schema.zookeeper;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.delegation.Lookups;
import com.moz.fiji.delegation.PriorityProvider;
import com.moz.fiji.schema.FijiURI;

/**
 * An interface for classes which can translate a {@link FijiURI} into a valid ZooKeeper ensemble
 * addresses. This layer of indirection is necessary so that test code may seamlessly use in-process
 * ZooKeeper clusters.
 */
@ApiAudience.Private
@ApiStability.Experimental
public interface ZooKeeperFactory extends PriorityProvider {

  /**
   * Provides a {@link ZooKeeperFactory}. There should only be a single {@code ZooKeeperFactory}
   * active per JVM, so {@link #get()} will always return the same object.
   */
  public static final class Provider {
    private static final ZooKeeperFactory INSTANCE =
        Lookups.getPriority(ZooKeeperFactory.class).lookup();

    /**
     * @return the {@link ZooKeeperFactory} with the highest priority.
     */
    public static ZooKeeperFactory get() {
      return INSTANCE;
    }

    /** Utility class may not be instantiated. */
    private Provider() {
    }
  }

  /**
   * Returns the ZooKeeper quorum address of the provided FijiURI in comma-separated host:port
   * (standard ZooKeeper) format. In almost all cases, {@link FijiURI#getZooKeeperEnsemble()} should
   * be preferred to this method.
   *
   * @param clusterURI of the FijiCluster for which to return the ZooKeeper quorum address.
   * @return the ZooKeeper quorum address of the Fiji cluster.
   */
  String getZooKeeperEnsemble(FijiURI clusterURI);
}
