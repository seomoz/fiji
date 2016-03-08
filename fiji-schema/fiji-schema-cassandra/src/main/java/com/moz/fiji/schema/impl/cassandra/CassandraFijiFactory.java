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

package com.moz.fiji.schema.impl.cassandra;

import java.io.IOException;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.delegation.Priority;
import com.moz.fiji.schema.FijiFactory;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.cassandra.CassandraFijiURI;

/** Factory for constructing instances of CassandraFiji. */
@ApiAudience.Private
public final class CassandraFijiFactory implements FijiFactory {

  /** Singleton C* Fiji factory. */
  private static CassandraFijiFactory singleton = null;

  /**
   * Getting for singleton instance.
   * @return The singleton CassandraFijiFactory.
   */
  public static CassandraFijiFactory get() {
    if (null == singleton) {
      singleton = new CassandraFijiFactory();
    }
    return singleton;
  }

  /** {@inheritDoc} */
  @Override
  public CassandraFiji open(FijiURI uri) throws IOException {
    Preconditions.checkArgument(uri instanceof CassandraFijiURI,
        "Fiji URI for a new Cassandra Fiji must be a CassandraFijiURI: '{}'.", uri);
    final CassandraFijiURI cassandraURI = (CassandraFijiURI) uri;
    return new CassandraFiji(cassandraURI, CassandraAdmin.create(cassandraURI));
  }

  /** {@inheritDoc} */
  @Override
  public CassandraFiji open(FijiURI uri, Configuration conf) throws IOException {
    return open(uri);
  }

  /** {@inheritDoc} */
  @Override
  public int getPriority(Map<String, String> runtimeHints) {
    // We don't use this mechanism anymore for providing FijiFactories.
    // Instead, the FijiURI provides it.
    return Priority.DISABLED;
  }
}
