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

import org.junit.Test;

import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiInvalidNameException;
import com.moz.fiji.schema.FijiNotInstalledException;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.cassandra.CassandraFijiClientTest;

/** Tests for FijiInstaller. */
public class TestCassandraFijiInstaller extends CassandraFijiClientTest {
  @Test
  public void testInstallThenUninstall() throws Exception {
    final FijiURI uri =
        FijiURI.newBuilder(createTestCassandraURI()).withInstanceName("test").build();
    CassandraFijiInstaller.get().install(uri, null);
    CassandraFijiInstaller.get().uninstall(uri, null);
  }

  @Test(expected = FijiInvalidNameException.class)
  public void testInstallNullInstance() throws Exception {
    final FijiURI uri =
        FijiURI.newBuilder(createTestCassandraURI()).withInstanceName(null).build();
    CassandraFijiInstaller.get().install(uri, null);
  }

  @Test(expected = FijiInvalidNameException.class)
  public void testUninstallNullInstance() throws Exception {
    final Fiji fiji = getFiji();
    final FijiURI uri = FijiURI.newBuilder(fiji.getURI()).withInstanceName(null).build();
    CassandraFijiInstaller.get().uninstall(uri, null);
  }

  @Test(expected = FijiNotInstalledException.class)
  public void testUninstallMissingInstance() throws Exception {
    final FijiURI uri =
        FijiURI.newBuilder(createTestCassandraURI()).withInstanceName("non_existent").build();
    CassandraFijiInstaller.get().uninstall(uri, null);
  }
}
