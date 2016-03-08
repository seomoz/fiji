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

package com.moz.fiji.schema.security;

import java.io.IOException;
import java.util.Set;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.FijiURI;

/**
 * {@link FijiSecurityManager} for Cassandra. Currently security is not implemented for Fiji
 * Cassandra.
 */
@ApiAudience.Private
public final class CassandraFijiSecurityManager implements FijiSecurityManager {

  /**
   * Factory method.
   *
   * @param uri of the Fiji instance.
   * @return a new security manager.
   * @throws IOException if there is a problem talking to Cassandra.
   */
  public static CassandraFijiSecurityManager create(final FijiURI uri) throws IOException {
    return new CassandraFijiSecurityManager();
  }

  /** {@inheritDoc} */
  @Override
  public void lock() throws IOException {
    throw new UnsupportedOperationException("Fiji Cassandra does not implement security.");
  }

  /** {@inheritDoc} */
  @Override
  public void unlock() throws IOException {
    throw new UnsupportedOperationException("Fiji Cassandra does not implement security.");
  }

  /** {@inheritDoc} */
  @Override
  public void grant(final FijiUser user, final FijiPermissions.Action action) throws IOException {
    throw new UnsupportedOperationException("Fiji Cassandra does not implement security.");
  }

  /** {@inheritDoc} */
  @Override
  public void grantAll(final FijiUser user) throws IOException {
    throw new UnsupportedOperationException("Fiji Cassandra does not implement security.");
  }

  /** {@inheritDoc} */
  @Override
  public void revoke(final FijiUser user, final FijiPermissions.Action action) throws IOException {
    throw new UnsupportedOperationException("Fiji Cassandra does not implement security.");
  }

  /** {@inheritDoc} */
  @Override
  public void revokeAll(final FijiUser user) throws IOException {
    throw new UnsupportedOperationException("Fiji Cassandra does not implement security.");
  }

  /** {@inheritDoc} */
  @Override
  public void reapplyInstancePermissions() throws IOException {
    throw new UnsupportedOperationException("Fiji Cassandra does not implement security.");
  }

  /** {@inheritDoc} */
  @Override
  public void applyPermissionsToNewTable(final FijiURI tableURI) throws IOException {
    throw new UnsupportedOperationException("Fiji Cassandra does not implement security.");
  }

  /** {@inheritDoc} */
  @Override
  public void grantInstanceCreator(final FijiUser user) throws IOException {
    throw new UnsupportedOperationException("Fiji Cassandra does not implement security.");
  }

  /** {@inheritDoc} */
  @Override
  public FijiPermissions getPermissions(final FijiUser user) throws IOException {
    throw new UnsupportedOperationException("Fiji Cassandra does not implement security.");
  }

  /** {@inheritDoc} */
  @Override
  public Set<FijiUser> listAllUsers() throws IOException {
    throw new UnsupportedOperationException("Fiji Cassandra does not implement security.");
  }

  /** {@inheritDoc} */
  @Override
  public void checkCurrentGrantAccess() throws IOException {
    throw new UnsupportedOperationException("Fiji Cassandra does not implement security.");
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException("Fiji Cassandra does not implement security.");
  }
}
