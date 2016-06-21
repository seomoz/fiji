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

package com.moz.fiji.schema.impl.hbase;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Joiner;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.InternalFijiError;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiAlreadyExistsException;
import com.moz.fiji.schema.FijiInstaller;
import com.moz.fiji.schema.FijiInvalidNameException;
import com.moz.fiji.schema.FijiSystemTable;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.hbase.HBaseFactory;
import com.moz.fiji.schema.hbase.FijiManagedHBaseTableName;
import com.moz.fiji.schema.impl.HBaseAdminFactory;
import com.moz.fiji.schema.impl.HTableInterfaceFactory;
import com.moz.fiji.schema.impl.Versions;
import com.moz.fiji.schema.security.FijiSecurityManager;
import com.moz.fiji.schema.util.ProtocolVersion;
import com.moz.fiji.schema.util.ResourceUtils;
import com.moz.fiji.schema.zookeeper.UsersTracker;
import com.moz.fiji.schema.zookeeper.ZooKeeperUtils;

/** Installs or uninstalls Fiji instances from an HBase cluster. */
@ApiAudience.Private
public final class HBaseFijiInstaller extends FijiInstaller {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseFijiInstaller.class);
  /** Singleton FijiInstaller. **/
  private static final HBaseFijiInstaller SINGLETON = new HBaseFijiInstaller();

  /** Constructs an HBaseFijiInstaller. */
  private HBaseFijiInstaller() {
    super();
  }

  /** {@inheritDoc} */
  @Override
  public void install(
      final FijiURI uri,
      final HBaseFactory hbaseFactory,
      final Map<String, String> properties,
      final Configuration conf
  ) throws IOException {
    if (uri.getInstance() == null) {
      throw new FijiInvalidNameException(String.format(
          "Fiji URI '%s' does not specify a Fiji instance name", uri));
    }

    final HBaseAdminFactory adminFactory = hbaseFactory.getHBaseAdminFactory(uri);
    final HTableInterfaceFactory tableFactory = hbaseFactory.getHTableInterfaceFactory(uri);

    // TODO: Factor this in HBaseFiji
    conf.set(HConstants.ZOOKEEPER_QUORUM, Joiner.on(",").join(uri.getZookeeperQuorumOrdered()));
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, uri.getZookeeperClientPort());

    final HBaseAdmin hbaseAdmin = adminFactory.create(conf);
    try {
      if (hbaseAdmin.tableExists(
          FijiManagedHBaseTableName.getSystemTableName(uri.getInstance()).toString())) {
        throw new FijiAlreadyExistsException(String.format(
            "Fiji instance '%s' already exists.", uri), uri);
      }
      LOG.info(String.format("Installing fiji instance '%s'.", uri));
      HBaseSystemTable.install(hbaseAdmin, uri, conf, properties, tableFactory);
      HBaseMetaTable.install(hbaseAdmin, uri);
      HBaseSchemaTable.install(hbaseAdmin, uri, conf, tableFactory);
      // Grant the current user all privileges on the instance just created, if security is enabled.
      final Fiji fiji = Fiji.Factory.open(uri, conf);
      try {
        if (fiji.isSecurityEnabled()) {
          FijiSecurityManager.Installer.installInstanceCreator(uri, conf, tableFactory);
        }
      } finally {
        fiji.release();
      }
    } finally {
      ResourceUtils.closeOrLog(hbaseAdmin);
    }
    LOG.info(String.format("Installed fiji instance '%s'.", uri));
  }

  /** {@inheritDoc} */
  @Override
  public void uninstall(
      final FijiURI uri,
      final HBaseFactory hbaseFactory,
      final Configuration conf
  ) throws IOException {
    if (uri.getInstance() == null) {
      throw new FijiInvalidNameException(String.format(
          "Fiji URI '%s' does not specify a Fiji instance name", uri));
    }
    final HBaseAdminFactory adminFactory = hbaseFactory.getHBaseAdminFactory(uri);
    final HTableInterfaceFactory htableFactory = hbaseFactory.getHTableInterfaceFactory(uri);

    // TODO: Factor this in HBaseFiji
    conf.set(HConstants.ZOOKEEPER_QUORUM, Joiner.on(",").join(uri.getZookeeperQuorumOrdered()));
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, uri.getZookeeperClientPort());

    LOG.info(String.format("Removing the fiji instance '%s'.", uri.getInstance()));

    final ProtocolVersion systemVersion = getSystemVersion(uri, conf, htableFactory);
    if (systemVersion.compareTo(Versions.SYSTEM_2_0) < 0) {
      uninstallSystem_1_0(uri, conf, adminFactory);
    } else if (systemVersion.compareTo(Versions.SYSTEM_2_0) == 0) {
      uninstallSystem_2_0(uri, conf, adminFactory);
    } else {
      throw new InternalFijiError(String.format("Unknown System version %s.", systemVersion));
    }

    LOG.info("Removed fiji instance '{}'.", uri.getInstance());
  }

  // CSOFF: MethodName
  /**
   * Uninstall a Fiji SYSTEM_1_0 instance.
   *
   * @param uri of instance.
   * @param conf configuration to connect to instance.
   * @param adminFactory to connect to instance.
   * @throws java.io.IOException on unrecoverable error.
   */
  private static void uninstallSystem_1_0(
      final FijiURI uri,
      final Configuration conf,
      final HBaseAdminFactory adminFactory
  ) throws IOException {
    final Fiji fiji = new HBaseFijiFactory().open(uri, conf);
    try {
      // If security is enabled, make sure the user has GRANT access on the instance
      // before uninstalling.
      if (fiji.isSecurityEnabled()) {
        FijiSecurityManager securityManager = fiji.getSecurityManager();
        try {
          securityManager.checkCurrentGrantAccess();
        } finally {
          securityManager.close();
        }
      }

      for (String tableName : fiji.getTableNames()) {
        LOG.debug("Deleting fiji table " + tableName + "...");
        fiji.deleteTable(tableName);
      }

      // Delete the user tables:
      final HBaseAdmin hbaseAdmin = adminFactory.create(conf);
      try {
        // Delete the system tables:
        HBaseSystemTable.uninstall(hbaseAdmin, uri);
        HBaseMetaTable.uninstall(hbaseAdmin, uri);
        HBaseSchemaTable.uninstall(hbaseAdmin, uri);
      } finally {
        hbaseAdmin.close();
      }
    } finally {
      fiji.release();
    }
  }

  /**
   * Uninstall a Fiji SYSTEM_2_0 instance.
   *
   * @param uri of instance.
   * @param conf configuration to connect to instance.
   * @param adminFactory to connect to instance.
   * @throws java.io.IOException on unrecoverable error.
   */
  private static void uninstallSystem_2_0(
      final FijiURI uri,
      final Configuration conf,
      final HBaseAdminFactory adminFactory
  ) throws IOException {
    final CuratorFramework zkClient = ZooKeeperUtils.getZooKeeperClient(uri);
    final String instanceZKPath = ZooKeeperUtils.getInstanceDir(uri).getPath();
    try {
      final UsersTracker usersTracker = ZooKeeperUtils.newInstanceUsersTracker(zkClient, uri);
      try {
        usersTracker.start();
        final Set<String> users = usersTracker.getUsers().keySet();
        if (!users.isEmpty()) {
          LOG.error(
              "Uninstalling Fiji instance '{}' with registered users."
                  + " Current registered users: {}. Stale instance metadata will remain in"
                  + " ZooKeeper at path {}.", uri.getInstance(), users, instanceZKPath);
        }
      } finally {
        usersTracker.close();
      }

      // The uninstall of tables from HBase is the same as System_1_0
      uninstallSystem_1_0(uri, conf, adminFactory);

      // Try to delete instance ZNodes from ZooKeeper
      ZooKeeperUtils.atomicRecursiveDelete(zkClient, instanceZKPath);
    } finally {
      zkClient.close();
    }
  }
  // CSON

  /**
   * Get the system version of an installed Fiji instance.
   *
   * @param instanceURI of Fiji instance.
   * @param conf to connect to instance.
   * @param htableFactory to connect to instance.
   * @return the system version.
   * @throws java.io.IOException if unrecoverable error.
   */
  private static ProtocolVersion getSystemVersion(
      final FijiURI instanceURI,
      final Configuration conf,
      final HTableInterfaceFactory htableFactory
  ) throws IOException {
    final FijiSystemTable systemTable = new HBaseSystemTable(instanceURI, conf, htableFactory);
    try {
      return systemTable.getDataVersion();
    } finally {
      systemTable.close();
    }
  }

  /**
   * Gets an instance of a FijiInstaller.
   *
   * @return An instance of a FijiInstaller.
   */
  public static HBaseFijiInstaller get() {
    return SINGLETON;
  }
}
