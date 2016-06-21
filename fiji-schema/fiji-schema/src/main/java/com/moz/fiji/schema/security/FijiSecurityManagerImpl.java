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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.security.access.AccessControlLists;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiSystemTable;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.hbase.FijiManagedHBaseTableName;
import com.moz.fiji.schema.impl.HTableInterfaceFactory;
import com.moz.fiji.schema.impl.Versions;
import com.moz.fiji.schema.impl.hbase.HBaseFiji;
import com.moz.fiji.schema.util.Lock;
import com.moz.fiji.schema.zookeeper.ZooKeeperLock;
import com.moz.fiji.schema.zookeeper.ZooKeeperUtils;

/**
 * The default implementation of FijiSecurityManager.
 *
 * <p>FijiSecurityManager manages access control for a Fiji instance.  It depends on ZooKeeper
 * locks to ensure atomicity of permissions operations.</p>
 *
 * <p>The current version of Fiji security (security-0.1) is instance-level only.  Users can have
 * READ, WRITE, and/or GRANT access on a Fiji instance.</p>
 */
@ApiAudience.Private
final class FijiSecurityManagerImpl implements FijiSecurityManager {
  private static final Logger LOG = LoggerFactory.getLogger(FijiSecurityManagerImpl.class);

  /** The Fiji instance this manages. */
  private final FijiURI mInstanceUri;

  /** A handle to the Fiji this manages. */
  private final Fiji mFiji;

  /** A handle to the HBaseAdmin of mFiji. */
  private final HBaseAdmin mAdmin;

  /** The system table for the instance this manages. */
  private final FijiSystemTable mSystemTable;

  /** The HBase ACL (Access Control List) table to use. */
  private final HTableInterface mAccessControlTable;

  /** ZooKeeper client connection responsible for creating instance locks. */
  private final CuratorFramework mZKClient;

  /** The zookeeper lock for this instance. */
  private final Lock mLock;

  /** The timeout, in seconds, to wait for ZooKeeper locks before throwing an exception. */
  private static final int LOCK_TIMEOUT = 10;

  /**
   * Constructs a new FijiSecurityManager for an instance, with the specified configuration.
   *
   * <p>A FijiSecurityManager cannot be constructed for an instance if the instance does not have a
   * security version greater than or equal to security-0.1 (that is, if security is not enabled).
   * </p>
   *
   * @param instanceUri is the URI of the instance this FijiSecurityManager will manage.
   * @param conf is the Hadoop configuration to use.
   * @param tableFactory is the table factory to use to access the HBase ACL table.
   * @throws IOException on I/O error.
   * @throws FijiSecurityException if the Fiji security version is not compatible with
   *     FijiSecurityManager.
   */
  FijiSecurityManagerImpl(
      FijiURI instanceUri,
      Configuration conf,
      HTableInterfaceFactory tableFactory) throws IOException {
    mInstanceUri = instanceUri;
    mFiji = Fiji.Factory.get().open(mInstanceUri);
    mSystemTable = mFiji.getSystemTable();

    // If the Fiji has security version lower than MIN_SECURITY_VERSION, then FijiSecurityManager
    // can't be instantiated.
    if (mSystemTable.getSecurityVersion().compareTo(Versions.MIN_SECURITY_VERSION) < 0) {
      mFiji.release();
      throw new FijiSecurityException("Cannot create a FijiSecurityManager for security version "
          + mSystemTable.getSecurityVersion() + ". Version must be "
          + Versions.MIN_SECURITY_VERSION + " or higher.");
    }

    mAdmin = ((HBaseFiji) mFiji).getHBaseAdmin();

    // TODO(SCHEMA-921): Security features should be moved into a bridge for CDH4.
    // Get the access control table.
    mAccessControlTable = tableFactory
        .create(conf, AccessControlLists.ACL_TABLE_NAME.getNameAsString());

    mZKClient = ZooKeeperUtils.getZooKeeperClient(mInstanceUri);
    mLock = new ZooKeeperLock(mZKClient, ZooKeeperUtils.getInstancePermissionsLock(instanceUri));
  }

  /** {@inheritDoc} */
  @Override
  public void lock() throws IOException {
    LOG.debug("Locking permissions for instance: '{}'.", mInstanceUri);
    boolean lockSuccessful = mLock.lock(LOCK_TIMEOUT);
    if (!lockSuccessful) {
      throw new FijiSecurityException("Acquiring lock on instance " + mInstanceUri
          + " timed out after " + LOCK_TIMEOUT + " seconds.");
    }
  }

  /** {@inheritDoc} */
  @Override
  public void unlock() throws IOException {
    LOG.debug("Unlocking permissions for instance: '{}'.", mInstanceUri);
    mLock.unlock();
  }

  /** {@inheritDoc} */
  @Override
  public void grant(FijiUser user, FijiPermissions.Action action)
      throws IOException {
    lock();
    try {
      grantWithoutLock(user, action);
    } finally {
      unlock();
    }
  }

  /** {@inheritDoc} */
  @Override
  public void grantAll(FijiUser user) throws IOException {
    lock();
    try {
      grantAllWithoutLock(user);
    } finally {
      unlock();
    }
  }

  /** {@inheritDoc} */
  @Override
  public void revoke(FijiUser user, FijiPermissions.Action action)
      throws IOException {
    lock();
    try {
      FijiPermissions currentPermissions = getPermissions(user);
      FijiPermissions newPermissions = currentPermissions.removeAction(action);
      updatePermissions(user, newPermissions);
      revokeInstancePermissions(user, FijiPermissions.newWithActions(Sets.newHashSet(action)));
    } finally {
      unlock();
    }
  }

  /** {@inheritDoc} */
  @Override
  public void revokeAll(FijiUser user) throws IOException {
    lock();
    try {
      updatePermissions(user, FijiPermissions.emptyPermissions());
      revokeInstancePermissions(
          user, FijiPermissions.newWithActions(Sets.newHashSet(FijiPermissions.Action.values())));
    } finally {
      unlock();
    }
  }

  /** {@inheritDoc} */
  @Override
  public void reapplyInstancePermissions() throws IOException {
    lock();
    try {
      Set<FijiUser> allUsers = listAllUsers();
      for (FijiUser user : allUsers) {
        FijiPermissions permissions = getPermissions(user);
        // Grant privileges the user should have.
        for (FijiPermissions.Action action : permissions.getActions()) {
          grant(user, action);
        }
        // Revoke privileges the user shouldn't have.
        Set<FijiPermissions.Action> forbiddenActions =
            Sets.difference(
                Sets.newHashSet(FijiPermissions.Action.values()),
                permissions.getActions());
        for (FijiPermissions.Action action : forbiddenActions) {
          revoke(user, action);
        }
      }
    } finally {
      unlock();
    }
  }

  /** {@inheritDoc} */
  @Override
  public void applyPermissionsToNewTable(FijiURI tableURI) throws IOException {
    // The argument must be for a table in the instance this manages.
    Preconditions.checkArgument(
        FijiURI.newBuilder(mInstanceUri).withTableName(tableURI.getTable()).build()
            .equals(tableURI));
    for (FijiUser user : listAllUsers()) {
      grantHTablePermissions(user.getName(),
          FijiManagedHBaseTableName
              .getFijiTableName(tableURI.getInstance(), tableURI.getTable()).toBytes(),
          getPermissions(user).toHBaseActions());
    }
  }

  /** {@inheritDoc} */
  @Override
  public void grantInstanceCreator(FijiUser user) throws IOException {
    lock();
    try {
      Set<FijiUser> currentGrantors = getUsersWithPermission(FijiPermissions.Action.GRANT);
      // This can only be called if there are no grantors, right when the instance is created.
      if (currentGrantors.size() != 0) {
        throw new FijiAccessException(
            "Cannot add user " + user
                + " to grantors as the instance creator for instance '"
                + mInstanceUri.toOrderedString()
                + "' because there are already grantors for this instance.");
      }
      Set<FijiUser> newGrantor = Collections.singleton(user);
      putUsersWithPermission(FijiPermissions.Action.GRANT, newGrantor);
      grantAllWithoutLock(user);
    } finally {
      unlock();
    }
    LOG.info("Creator permissions on instance '{}' granted to user {}.",
        mInstanceUri,
        user.getName());
  }

  /** {@inheritDoc} */
  @Override
  public FijiPermissions getPermissions(FijiUser user) throws IOException {
    FijiPermissions result = FijiPermissions.emptyPermissions();

    for (FijiPermissions.Action action : FijiPermissions.Action.values()) {
      Set<FijiUser> usersWithAction = getUsersWithPermission(action);
      if (usersWithAction.contains(user)) {
        result = result.addAction(action);
      }
    }

    return result;
  }

  /** {@inheritDoc} */
  @Override
  public Set<FijiUser> listAllUsers() throws IOException {
    Set<FijiUser> allUsers = new HashSet<FijiUser>();
    for (FijiPermissions.Action action : FijiPermissions.Action.values()) {
      allUsers.addAll(getUsersWithPermission(action));
    }

    return allUsers;
  }

  /** {@inheritDoc} */
  @Override
  public void checkCurrentGrantAccess() throws IOException {
    FijiUser currentUser = FijiUser.getCurrentUser();
    if (!getPermissions(currentUser).allowsAction(FijiPermissions.Action.GRANT)) {
      throw new FijiAccessException("User " + currentUser.getName()
          + " does not have GRANT access for instance " + mInstanceUri.toString() + ".");
    }
  }

  /**
   * Grant action to a user, without locking the instance.  When using this method, one must lock
   * the instance before, and unlock it after.
   *
   * @param user User to grant action to.
   * @param action Action to grant to user.
   * @throws IOException on I/O error.
   */
  private void grantWithoutLock(FijiUser user, FijiPermissions.Action action) throws IOException {
    FijiPermissions currentPermissions = getPermissions(user);
    FijiPermissions newPermissions = currentPermissions.addAction(action);
    grantInstancePermissions(user, newPermissions);
  }

  /**
   * Grants all actions to a user, without locking the instance.  When using this method, one must
   * lock the instance before, and unlock it after.
   *
   * @param user User to grant all actions to.
   * @throws IOException on I/O error.
   */
  private void grantAllWithoutLock(FijiUser user)
      throws IOException {
    LOG.debug("Granting all permissions to user {} on instance '{}'.",
        user.getName(),
        mInstanceUri.toOrderedString());
    FijiPermissions newPermissions = getPermissions(user);
    for (FijiPermissions.Action action : FijiPermissions.Action.values()) {
      newPermissions = newPermissions.addAction(action);
    }
    grantInstancePermissions(user, newPermissions);
  }

  /**
   * Updates the permissions in the Fiji system table for a user on this Fiji instance.
   *
   * <p>Use {@link #grantInstancePermissions(FijiUser, FijiPermissions)}
   * or {@link #revokeInstancePermissions(FijiUser, FijiPermissions)}instead for updating the
   * permissions in HBase as well as in the Fiji system table.</p>
   *
   * @param user whose permissions to update.
   * @param permissions to be applied to this user.
   * @throws IOException If there is an I/O error.
   */
  private void updatePermissions(FijiUser user, FijiPermissions permissions)
      throws IOException {
    checkCurrentGrantAccess();
    for (FijiPermissions.Action action : FijiPermissions.Action.values()) {
      Set<FijiUser> permittedUsers = getUsersWithPermission(action);
      if (permissions.allowsAction(action)) {
        permittedUsers.add(user);
      } else {
        permittedUsers.remove(user);
      }
      putUsersWithPermission(action, permittedUsers);
    }
  }

  /**
   * Gets the users with permission 'action' in this instance, as recorded in the Fiji system
   * table.
   *
   * @param action specifying the permission to get the users of.
   * @return the list of users with that permission.
   * @throws IOException on I/O exception.
   */
  private Set<FijiUser> getUsersWithPermission(FijiPermissions.Action action) throws IOException {
    byte[] serialized = mSystemTable.getValue(action.getStringKey());
    if (null == serialized) {
      // If the key doesn't exist, no users have been put with that permission yet.
      return new HashSet<FijiUser>();
    } else {
      return FijiUser.deserializeFijiUsers(serialized);
    }
  }

  /**
   * Records a set of users as permitted to have action 'action', by recording them in the Fiji
   * system table.
   *
   * @param action to put the set of users into.
   * @param users to put to that permission.
   * @throws IOException on I/O exception.
   */
  private void putUsersWithPermission(
      FijiPermissions.Action action,
      Set<FijiUser> users)
      throws IOException {
    mSystemTable.putValue(action.getStringKey(), FijiUser.serializeFijiUsers(users));
  }

  /**
   * Changes the permissions of an instance, by granting the permissions on of all the Fiji meta
   * tables.
   *
   * <p>Permissions should be updated with #updatePermissions before calling this method.</p>
   *
   * @param user is the User to whom the permissions are being granted.
   * @param permissions is the new permissions granted to the user.
   * @throws IOException on I/O error.
   */
  private void grantInstancePermissions(
      FijiUser user,
      FijiPermissions permissions) throws IOException {
    LOG.info("Changing user permissions for user {} on instance {} to actions {}.",
        user,
        mInstanceUri,
        permissions.getActions());

    // Record the changes in the system table.
    updatePermissions(user, permissions);

    // Change permissions of Fiji system tables in HBase.
    FijiPermissions systemTablePermissions;
    // If this is GRANT permission, also add WRITE access to the permissions in the system table.
    if (permissions.allowsAction(FijiPermissions.Action.GRANT)) {
      systemTablePermissions =
          permissions.addAction(FijiPermissions.Action.WRITE);
    } else {
      systemTablePermissions = permissions;
    }
    grantHTablePermissions(user.getName(),
        FijiManagedHBaseTableName.getSystemTableName(mInstanceUri.getInstance()).toBytes(),
        systemTablePermissions.toHBaseActions());

    // Change permissions of the other Fiji meta tables.
    grantHTablePermissions(user.getName(),
        FijiManagedHBaseTableName.getMetaTableName(mInstanceUri.getInstance()).toBytes(),
        permissions.toHBaseActions());
    grantHTablePermissions(user.getName(),
        FijiManagedHBaseTableName.getSchemaIdTableName(mInstanceUri.getInstance()).toBytes(),
        permissions.toHBaseActions());
    grantHTablePermissions(user.getName(),
        FijiManagedHBaseTableName.getSchemaHashTableName(mInstanceUri.getInstance()).toBytes(),
        permissions.toHBaseActions());

    // Change permissions of all Fiji tables in this instance in HBase.
    Fiji fiji = Fiji.Factory.open(mInstanceUri);
    try {
      for (String fijiTableName : fiji.getTableNames()) {
        byte[] fijiHTableNameBytes =
            FijiManagedHBaseTableName.getFijiTableName(
                mInstanceUri.getInstance(),
                fijiTableName
            ).toBytes();
        grantHTablePermissions(user.getName(),
            fijiHTableNameBytes,
            permissions.toHBaseActions());
      }
    } finally {
      fiji.release();
    }
    LOG.debug("Permissions on instance {} successfully changed.", mInstanceUri);
  }

  /**
   * Changes the permissions of an instance, by revoking the permissions on of all the Fiji meta
   * tables.
   *
   * <p>Permissions should be updated with #updatePermissions before calling this method.</p>
   *
   * @param user User from whom the permissions are being revoked.
   * @param permissions Permissions to be revoked from the user.
   * @throws IOException on I/O error.
   */
  private void revokeInstancePermissions(
      FijiUser user,
      FijiPermissions permissions) throws IOException {
    // If GRANT permission is revoked, also remove WRITE access to the system table.
    FijiPermissions systemTablePermissions;
    if (permissions.allowsAction(FijiPermissions.Action.GRANT)) {
      systemTablePermissions =
          permissions.addAction(FijiPermissions.Action.WRITE);
    } else {
      systemTablePermissions = permissions;
    }
    revokeHTablePermissions(user.getName(),
        FijiManagedHBaseTableName.getSystemTableName(mInstanceUri.getInstance()).toBytes(),
        systemTablePermissions.toHBaseActions());

    // Change permissions of the other Fiji meta tables.
    revokeHTablePermissions(user.getName(),
        FijiManagedHBaseTableName.getMetaTableName(mInstanceUri.getInstance()).toBytes(),
        permissions.toHBaseActions());
    revokeHTablePermissions(user.getName(),
        FijiManagedHBaseTableName.getSchemaIdTableName(mInstanceUri.getInstance()).toBytes(),
        permissions.toHBaseActions());

    // Change permissions of all Fiji tables in this instance in HBase.
    for (String fijiTableName : mFiji.getTableNames()) {
      byte[] fijiHTableNameBytes =
          FijiManagedHBaseTableName.getFijiTableName(
              mInstanceUri.getInstance(),
              fijiTableName
          ).toBytes();
      revokeHTablePermissions(user.getName(),
          fijiHTableNameBytes,
          permissions.toHBaseActions());
    }
    LOG.debug("Permissions {} on instance '{}' successfully revoked from user {}.",
        permissions,
        mInstanceUri.toOrderedString(),
        user);
  }

  /**
   * Grants the actions to user on an HBase table.
   *
   * @param hUser HBase byte representation of the user whose permissions to change.
   * @param hTableName the HBase table to change permissions on.
   * @param hActions for the user on the table.
   * @throws IOException on I/O error, for example if security is not enabled.
   */
  private void grantHTablePermissions(
      String hUser,
      byte[] hTableName,
      Action[] hActions) throws IOException {
    LOG.debug("Changing user permissions for user {} on table {} to HBase Actions {}.",
        hUser,
        Bytes.toString(hTableName),
        Arrays.toString(hActions));
    LOG.debug("Disabling table {}.", Bytes.toString(hTableName));
    mAdmin.disableTable(hTableName);
    LOG.debug("Table {} disabled.", Bytes.toString(hTableName));

    // Grant the permissions.
    AccessControlProtos.AccessControlService.BlockingInterface protocol =
        AccessControlProtos.AccessControlService.newBlockingStub(
            mAccessControlTable.coprocessorService(HConstants.EMPTY_START_ROW)
        );
    try {
      ProtobufUtil.grant(protocol, hUser, TableName.valueOf(hTableName), null, null, hActions);
    } catch (Throwable throwable) {
      throw new FijiSecurityException("Encountered exception while granting access.",
          throwable);
    }

    LOG.debug("Enabling table {}.", Bytes.toString(hTableName));
    mAdmin.enableTable(hTableName);
    LOG.debug("Table {} enabled.", Bytes.toString(hTableName));
  }

  /**
   * Revokes the actions from user on an HBase table.
   *
   * @param hUser HBase byte representation of the user whose permissions to change.
   * @param hTableName the HBase table to change permissions on.
   * @param hActions for the user on the table.
   * @throws IOException on I/O error, for example if security is not enabled.
   */
  private void revokeHTablePermissions(
      String hUser,
      byte[] hTableName,
      Action[] hActions) throws IOException {
    LOG.debug("Revoking user permissions for user {} on table {} to HBase Actions {}.",
        hUser,
        Bytes.toString(hTableName),
        Arrays.toString(hActions));
    LOG.debug("Disabling table {}.", Bytes.toString(hTableName));
    mAdmin.disableTable(hTableName);
    LOG.debug("Table {} disabled.", Bytes.toString(hTableName));
    // Revoke the permissions.
    AccessControlProtos.AccessControlService.BlockingInterface protocol =
        AccessControlProtos.AccessControlService.newBlockingStub(
            mAccessControlTable.coprocessorService(HConstants.EMPTY_START_ROW)
        );
    try {
      ProtobufUtil.revoke(protocol, hUser, TableName.valueOf(hTableName), null, null, hActions);
    } catch (Throwable throwable) {
      throw new FijiSecurityException("Encountered exception while revoking access.",
          throwable);
    }
    LOG.debug("Enabling table {}.", Bytes.toString(hTableName));
    mAdmin.enableTable(hTableName);
    LOG.debug("Table {} enabled.", Bytes.toString(hTableName));
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    mFiji.release();
    mLock.close();
    mZKClient.close();
  }
}
