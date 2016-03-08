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

package com.moz.fiji.schema.impl.cassandra;

import java.io.IOException;
import java.util.Map;

import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiAlreadyExistsException;
import com.moz.fiji.schema.FijiInstaller;
import com.moz.fiji.schema.FijiInvalidNameException;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.cassandra.CassandraFijiURI;
import com.moz.fiji.schema.hbase.HBaseFactory;

/** Installs or uninstalls Fiji instances from an Cassandra cluster. */
@ApiAudience.Public
@ApiStability.Evolving
public final class CassandraFijiInstaller extends FijiInstaller {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraFijiInstaller.class);
  /** Singleton FijiInstaller. **/
  private static final CassandraFijiInstaller SINGLETON = new CassandraFijiInstaller();

  /** Constructs a CassandraFijiInstaller. */
  private CassandraFijiInstaller() {
    super();
  }

  /** {@inheritDoc} */
  @Override
  public void install(
      FijiURI uri,
      HBaseFactory hbaseFactory,
      Map<String, String> properties,
      Configuration conf
    ) throws IOException {
    Preconditions.checkArgument(uri instanceof CassandraFijiURI,
        "Fiji URI for a new Cassandra Fiji installation must be a CassandraFijiURI: '{}'.", uri);
    final CassandraFijiURI cassandraURI = (CassandraFijiURI) uri;
    if (cassandraURI.getInstance() == null) {
      throw new FijiInvalidNameException(String.format(
          "Fiji URI '%s' does not specify a Fiji instance name", cassandraURI));
    }
    try {
      LOG.info(String.format("Installing Cassandra Fiji instance '%s'.", cassandraURI));

      try (CassandraAdmin admin = CassandraAdmin.create(cassandraURI)) {
        // Install the system, meta, and schema tables.
        CassandraSystemTable.install(admin, cassandraURI, properties);
        CassandraMetaTable.install(admin, cassandraURI);
        CassandraSchemaTable.install(admin, cassandraURI);
      }

      // Grant the current user all privileges on the instance just created, if security is enabled.
      final Fiji fiji = CassandraFijiFactory.get().open(cassandraURI);
      try {
        if (fiji.isSecurityEnabled()) {
          throw new UnsupportedOperationException("Fiji Cassandra does not implement security.");
        }
      } finally {
        fiji.release();
      }
    } catch (AlreadyExistsException aee) {
      throw new FijiAlreadyExistsException(String.format(
          "Cassandra Fiji instance '%s' already exists.", cassandraURI), cassandraURI);
    }
    LOG.info(String.format("Installed Cassandra Fiji instance '%s'.", cassandraURI));
  }

  /** {@inheritDoc} */
  @Override
  public void uninstall(
      FijiURI uri,
      HBaseFactory hbaseFactory,
      Configuration conf
  ) throws IOException {
    Preconditions.checkArgument(uri instanceof CassandraFijiURI,
        "Fiji URI for a new Cassandra Fiji installation must be a CassandraFijiURI: '{}'.", uri);
    final CassandraFijiURI cassandraURI = (CassandraFijiURI) uri;
    if (cassandraURI.getInstance() == null) {
      throw new FijiInvalidNameException(String.format(
          "Fiji URI '%s' does not specify a Fiji instance name", cassandraURI));
    }

    LOG.info(String.format("Uninstalling Fiji instance '%s'.", cassandraURI.getInstance()));

    final Fiji fiji = CassandraFijiFactory.get().open(cassandraURI);
    try {
      // TODO (SCHEMA-706): Add security checks when we have a plan for security in Cassandra Fiji.

      for (String tableName : fiji.getTableNames()) {
        LOG.info("Deleting Fiji table {}.", tableName);
        fiji.deleteTable(tableName);
      }
      // Delete the user tables:
      try (CassandraAdmin admin = CassandraAdmin.create(cassandraURI)) {
        // Delete the system tables:
        CassandraSystemTable.uninstall(admin, cassandraURI);
        CassandraMetaTable.uninstall(admin, cassandraURI);
        CassandraSchemaTable.uninstall(admin, cassandraURI);
        admin.deleteKeyspace();
      }

    } finally {
      fiji.release();
    }
    LOG.info(String.format("Fiji instance '%s' uninstalled.", cassandraURI.getInstance()));
  }

  /**
   * Gets an instance of a CassandraFijiInstaller.
   *
   * @return An instance of a CassandraFijiInstaller.
   */
  public static CassandraFijiInstaller get() {
    return SINGLETON;
  }
}
