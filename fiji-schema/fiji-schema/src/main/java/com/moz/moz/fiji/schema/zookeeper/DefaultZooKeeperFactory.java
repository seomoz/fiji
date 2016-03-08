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

import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.Inheritance;
import com.moz.fiji.delegation.Priority;
import com.moz.fiji.schema.FijiURI;

/**
 * A {@link ZooKeeperFactory} which assumes a FijiURI is pointing to an existing, remote, ZooKeeper
 * ensemble.  This {@link ZooKeeperFactory} implementation will be used in all non-unit-test
 * situations.
 */
@ApiAudience.Private
@Inheritance.Sealed
public class DefaultZooKeeperFactory implements ZooKeeperFactory {

  /** {@inheritDoc}. */
  @Override
  public String getZooKeeperEnsemble(FijiURI uri) {
    final List<String> zkHosts = Lists.newArrayList();
    for (String host : uri.getZookeeperQuorum()) {
      zkHosts.add(String.format("%s:%s", host, uri.getZookeeperClientPort()));
    }
    return Joiner.on(",").join(zkHosts);
  }

  @Override
  public int getPriority(Map<String, String> runtimeHints) {
    // Default priority; should be used unless overridden by a higher priority test provider.
    return Priority.NORMAL;
  }
}
