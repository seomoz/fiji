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

package com.moz.fiji.schema.impl.hbase;

import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Test;

import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiInstaller;
import com.moz.fiji.schema.FijiInvalidNameException;
import com.moz.fiji.schema.FijiNotInstalledException;
import com.moz.fiji.schema.FijiURI;

/** Tests for HBaseFijiInstaller. */
public class TestHBaseFijiInstaller extends FijiClientTest {
  @Test
  public void testInstallThenUninstall() throws Exception {
    final Configuration conf = HBaseConfiguration.create();
    final FijiURI uri = FijiURI.newBuilder(createTestHBaseURI()).withInstanceName("test").build();
    FijiInstaller.get().install(uri, conf);
    FijiInstaller.get().uninstall(uri, conf);
  }

  @Test
  public void testInstallNullInstance() throws Exception {
    final Configuration conf = HBaseConfiguration.create();
    final FijiURI uri = createTestHBaseURI();
    try {
      FijiInstaller.get().install(uri, conf);
      fail("An exception should have been thrown.");
    } catch (FijiInvalidNameException kine) {
      assertEquals(
          String.format("Fiji URI '%s' does not specify a Fiji instance name", uri.toString()),
          kine.getMessage());
    }
  }

  @Test
  public void testUninstallNullInstance() throws Exception {
    final Configuration conf = HBaseConfiguration.create();
    final FijiURI uri = FijiURI.newBuilder("fiji://.fake.fiji-installer/").build();
    try {
      FijiInstaller.get().uninstall(uri, conf);
      fail("An exception should have been thrown.");
    } catch (FijiInvalidNameException kine) {
      assertEquals(
          String.format("Fiji URI '%s' does not specify a Fiji instance name", uri.toString()),
          kine.getMessage());
    }
  }

  @Test
  public void testUninstallMissingInstance() throws Exception {
    final Configuration conf = HBaseConfiguration.create();
    final FijiURI uri = FijiURI.newBuilder(createTestHBaseURI())
        .withInstanceName("anInstanceThatNeverExisted")
        .build();
    try {
      FijiInstaller.get().uninstall(uri, conf);
      fail("An exception should have been thrown.");
    } catch (FijiNotInstalledException knie) {
      assertTrue(Pattern.matches(
          "Fiji instance fiji://.*/anInstanceThatNeverExisted/ is not installed\\.",
          knie.getMessage()));
    }
  }

  @Test
  public void testUninstallingInstanceWithUsersDoesNotFail() throws Exception {
    final Configuration conf = HBaseConfiguration.create();
    final FijiURI uri = FijiURI.newBuilder(createTestHBaseURI()).withInstanceName("test").build();
    final FijiInstaller installer = FijiInstaller.get();
    installer.install(uri, conf);
    Fiji fiji = Fiji.Factory.get().open(uri);
    try {
      installer.uninstall(uri, conf);
    } finally {
      fiji.release();
    }
  }
}
