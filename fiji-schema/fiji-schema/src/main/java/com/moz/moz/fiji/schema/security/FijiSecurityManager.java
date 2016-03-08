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

package com.moz.fiji.schema.security;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.annotations.Inheritance;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.impl.HTableInterfaceFactory;

/**
 * FijiSecurityManager manages access control for a Fiji instance.
 *
 * <p>The current version of Fiji security (security-0.1) is instance-level only.  Users can have
 * READ, WRITE, and/or GRANT access on a Fiji instance.</p>
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Inheritance.Sealed
public interface FijiSecurityManager extends Closeable {
  /**
   * Factory for creating FijiSecurityManagers.
   */
  public static final class Factory {
    /**
     * Constructs a new FijiSecurityManager for an instance, with the specified configuration.
     *
     * <p>A FijiSecurityManager cannot be constructed for an instance if the instance has a security
     * version of security-0.0 (that is, if it is not installed).</p>
     *
     * <p>FijiSecurityManagers should be closed when not used anymore, using {@link #close()}</p>
     *
     * <p>If you have a Fiji instance, you can get a new FijiSecurityManager using
     * {@link com.moz.fiji.schema.Fiji#getSecurityManager()}.</p>
     *
     * @param instanceUri is the URI of the instance this FijiSecurityManager will manage.
     * @param conf is the Hadoop configuration to use.
     * @param tableFactory to use to access the HBase ACL table.
     * @return a new FijiSecurityManager for the instance specified, and with the configuration
     *     specified.
     * @throws IOException on I/O error.
     */
    public static FijiSecurityManager create(
        FijiURI instanceUri,
        Configuration conf,
        HTableInterfaceFactory tableFactory) throws IOException {
      return new FijiSecurityManagerImpl(instanceUri, conf, tableFactory);
    }
  }

  /**
   * Installer used for installing Fiji security on a Fiji instance.
   */
  public static final class Installer {
    /**
     * Grants all permissions on an instance, the first time it's installed.
     *
     * @param instanceUri of the instance being installed.
     * @param conf Hadoop configuration.
     * @param tableFactory to use to access the HBase ACL table.
     * @throws IOException if the specified instance does not exist or on other I/O error.
     */
    public static void installInstanceCreator(
        FijiURI instanceUri,
        Configuration conf,
        HTableInterfaceFactory tableFactory) throws  IOException {
      Factory
          .create(instanceUri, conf, tableFactory)
          .grantInstanceCreator(FijiUser.getCurrentUser());
    }
  }

  /**
   * Locks the Fiji instance managed by this.
   *
   * <p>This locks instances across all instances of FijiSecurityManager in all processes.</p>
   *
   * @throws IOException on I/O error.
   */
  void lock() throws IOException;

  /**
   * Unlocks the Fiji instance managed by this.
   *
   * @throws IOException on I/O error.
   */
  void unlock() throws IOException;

  /**
   * Grants a user permissions to perform an action.
   *
   * @param user to grant permission to.
   * @param action to grant permission to perform.
   * @throws IOException on I/O error.
   */
  void grant(FijiUser user, FijiPermissions.Action action) throws IOException;

  /**
   * Grants a user permissions to perform all actions.  This is used internally as well, to grant
   * the creator of a table permissions to perform all actions.
   *
   * @param user to grant all permissions to.
   * @throws IOException on I/O error.
   */
  void grantAll(FijiUser user) throws IOException;

  /**
   * Revokes permissions from a user to perform an action.
   *
   * @param user to revoke permission from.
   * @param action to revoke permission to perform.
   * @throws IOException on I/O error.
   */
  void revoke(FijiUser user, FijiPermissions.Action action) throws IOException;

  /**
   * Revokes permissions to all actions on this instance for a user.
   *
   * @param user to revoke all permissions from.
   * @throws IOException on I/O error.
   */
  void revokeAll(FijiUser user) throws IOException;

  /**
   * Reapplies permissions stored in the Fiji system table to this instance, including all tables
   * in the instance.
   *
   * @throws IOException on I/O error.
   */
  void reapplyInstancePermissions() throws IOException;

  /**
   * Applies the correct permissions to a newly-created table in the instance managed  by this
   * FijiSecurityManager.  Fiji uses this when creating a table.  This should not be called outside
   * of fiji-schema.
   *
   * <p>Callers of this method should lock the Fiji instance using #lock first, and unlock after
   * using #unlock.</p>
   *
   * @param tableURI of the table that was just created.  Must be a URI specifying a table in the
   *     instance managed by this FijiSecurityManager.
   * @throws IOException on I/O error.
   */
  void applyPermissionsToNewTable(FijiURI tableURI) throws IOException;

  /**
   * Adds the creator of this instance as the initial user with GRANT access on this instance.
   *
   * <p>Throws a {@link FijiAccessException} if there are already grantors.</p>
   *
   * @param user to give GRANT access.
   * @throws IOException on I/O error.
   */
  void grantInstanceCreator(FijiUser user) throws IOException;

  /**
   * Gets the permissions for a user on this Fiji instance, as stored in the system table.
   *
   * @param user whose permissions to get.
   * @return the permissions of user.  If the user is not registered in Fiji at all,
   *     returns an empty FijiPermissions.
   * @throws IOException If there is an I/O error.
   */
  FijiPermissions getPermissions(FijiUser user) throws IOException;

  /**
   * Lists all users with any permissions on this instance.  Users with no permission are not
   * included.
   *
   * @return all users with any permissions on this instance.
   * @throws IOException on I/O error.
   */
  Set<FijiUser> listAllUsers() throws IOException;

  /**
   * Checks whether the current user has grant access, throwing a FijiAccessException if it
   * doesn't.
   *
   * @throws IOException on I/O error.
   */
  void checkCurrentGrantAccess() throws IOException;
}
