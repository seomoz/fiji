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

package com.moz.fiji.schema.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.junit.Test;

import com.moz.fiji.schema.KConstants;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiURIException;

public class TestCassandraFijiURI {
  @Test
  public void testCassandraUri() {
    final CassandraFijiURI uri = CassandraFijiURI.newBuilder(
        "fiji-cassandra://zkhost:1234/chost:5678").build();
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("chost", uri.getContactPoints().get(0));
    assertEquals(5678, uri.getContactPort());
    assertEquals(null, uri.getInstance());
    assertEquals(null, uri.getTable());
    assertEquals(null, uri.getUsername());
    assertEquals(null, uri.getPassword());
    assertTrue(uri.getColumns().isEmpty());
  }

  @Test
  public void testCassandraUriWithUsernameAndPassword() {
    final CassandraFijiURI uri = CassandraFijiURI.newBuilder(
        "fiji-cassandra://zkhost:1234/sallycinnamon:password@chost:5678").build();
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("chost", uri.getContactPoints().get(0));
    assertEquals(5678, uri.getContactPort());
    assertEquals(null, uri.getInstance());
    assertEquals(null, uri.getTable());
    assertEquals("sallycinnamon", uri.getUsername());
    assertEquals("password", uri.getPassword());
    assertTrue(uri.getColumns().isEmpty());
  }

  @Test
  public void testCassandraUriWithUsernameAndPasswordMultipleHosts() {
    final CassandraFijiURI uri = CassandraFijiURI.newBuilder(
        "fiji-cassandra://zkhost:1234/sallycinnamon:password@(chost1,chost2):5678").build();
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("chost1", uri.getContactPoints().get(0));
    assertEquals("chost2", uri.getContactPoints().get(1));
    assertEquals(5678, uri.getContactPort());
    assertEquals(null, uri.getInstance());
    assertEquals(null, uri.getTable());
    assertEquals("sallycinnamon", uri.getUsername());
    assertEquals("password", uri.getPassword());
    assertTrue(uri.getColumns().isEmpty());
  }

  @Test
  public void testCassandraUriDoubleApersand() {
    final String uriString = "fiji-cassandra://zkhost:1234/sally@cinnamon@chost:5678";
    try {
      final CassandraFijiURI uri = CassandraFijiURI.newBuilder(uriString).build();
      fail("An exception should have been thrown.");
    } catch (FijiURIException kurie) {
      assertEquals(String.format(
              "Invalid Fiji URI: '%s' : Cannot have more than one '@' in URI authority",
              uriString),
          kurie.getMessage());
    }
  }

  @Test
  public void testCassandraUriTooManyColons() {
    final String uriString = "fiji-cassandra://zkhost:1234/sally:cinnamon:foo@chost:5678";
    try {
      final CassandraFijiURI uri = CassandraFijiURI.newBuilder(uriString).build();
      fail("An exception should have been thrown.");
    } catch (FijiURIException kurie) {
      assertEquals(String.format(
              "Invalid Fiji URI: '%s' : Cannot have more than one ':' in URI user info",
              uriString),
          kurie.getMessage());
    }
  }

  @Test
  public void testCassandraUriWithUsername() {
    final String uriString = "fiji-cassandra://zkhost:1234/sallycinnamon@chost:5678";
    try {
      final CassandraFijiURI uri = CassandraFijiURI.newBuilder(uriString).build();
      fail("An exception should have been thrown.");
    } catch (FijiURIException kurie) {
      assertEquals(String.format(
          "Invalid Fiji URI: '%s' : Cassandra Fiji URIs do not support a username without a "
              + "password.",
          uriString),
          kurie.getMessage());
    }
  }

  @Test
  public void testFijiInstanceUri() {
    final CassandraFijiURI uri = CassandraFijiURI.newBuilder(
        "fiji-cassandra://zkhost:1234/chost:5678/instance").build();
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("chost", uri.getContactPoints().get(0));
    assertEquals(5678, uri.getContactPort());
    assertEquals("instance", uri.getInstance());
    assertEquals(null, uri.getTable());
    assertEquals(null, uri.getUsername());
    assertEquals(null, uri.getPassword());
    assertTrue(uri.getColumns().isEmpty());
  }

  @Test
  public void testSingleHost() {
    final CassandraFijiURI uri = CassandraFijiURI
        .newBuilder("fiji-cassandra://zkhost:1234/chost:5678/instance/table/col").build();
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("chost", uri.getContactPoints().get(0));
    assertEquals(5678, uri.getContactPort());
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
    assertEquals(null, uri.getUsername());
    assertEquals(null, uri.getPassword());
  }

  @Test
  public void testSingleHostGroupColumn() {
    final CassandraFijiURI uri =
        CassandraFijiURI.newBuilder(
            "fiji-cassandra://zkhost:1234/chost:5678/instance/table/family:qualifier").build();
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("chost", uri.getContactPoints().get(0));
    assertEquals(5678, uri.getContactPort());
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("family:qualifier", uri.getColumns().get(0).getName());
    assertEquals(null, uri.getUsername());
    assertEquals(null, uri.getPassword());
  }

  @Test
  public void testSingleHostDefaultInstance() {
    final CassandraFijiURI uri = CassandraFijiURI.newBuilder(
        "fiji-cassandra://zkhost:1234/chost:5678/default/table/col").build();
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(1, uri.getZookeeperQuorum().size());
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("chost", uri.getContactPoints().get(0));
    assertEquals(5678, uri.getContactPort());
    assertEquals("default", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
    assertEquals(null, uri.getUsername());
    assertEquals(null, uri.getPassword());
  }

  @Test
  public void testSingleHostDefaultPort() {
    final CassandraFijiURI uri = CassandraFijiURI.newBuilder(
        "fiji-cassandra://zkhost/chost:5678/instance/table/col").build();
    assertEquals(1, uri.getZookeeperQuorum().size());
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(CassandraFijiURI.DEFAULT_ZOOKEEPER_CLIENT_PORT, uri.getZookeeperClientPort());
    assertEquals("chost", uri.getContactPoints().get(0));
    assertEquals(5678, uri.getContactPort());
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
    assertEquals(null, uri.getUsername());
    assertEquals(null, uri.getPassword());
  }

  @Test
  public void testMultipleHosts() {
    final CassandraFijiURI uri = CassandraFijiURI
        .newBuilder(
            "fiji-cassandra://(zkhost1,zkhost2):1234/(chost1,chost2):5678/instance/table/col")
        .build();
    assertEquals("zkhost1", uri.getZookeeperQuorum().get(0));
    assertEquals("zkhost2", uri.getZookeeperQuorum().get(1));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("chost1", uri.getContactPoints().get(0));
    assertEquals("chost2", uri.getContactPoints().get(1));
    assertEquals(5678, uri.getContactPort());
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
    assertEquals(null, uri.getUsername());
    assertEquals(null, uri.getPassword());
  }

  @Test
  public void testMultipleHostsDefaultPort() {
    final CassandraFijiURI uri = CassandraFijiURI.newBuilder(
        "fiji-cassandra://zkhost1,zkhost2/chost:5678/instance/table/col").build();
    assertEquals("zkhost1", uri.getZookeeperQuorum().get(0));
    assertEquals("zkhost2", uri.getZookeeperQuorum().get(1));
    assertEquals(CassandraFijiURI.DEFAULT_ZOOKEEPER_CLIENT_PORT, uri.getZookeeperClientPort());
    assertEquals("chost", uri.getContactPoints().get(0));
    assertEquals(5678, uri.getContactPort());
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
    assertEquals(null, uri.getUsername());
    assertEquals(null, uri.getPassword());
  }

  @Test
  public void testMultipleHostsDefaultPortDefaultInstance() {
    final CassandraFijiURI uri = CassandraFijiURI.newBuilder(
        "fiji-cassandra://zkhost1,zkhost2/chost:5678/default/table/col").build();
    assertEquals("zkhost1", uri.getZookeeperQuorum().get(0));
    assertEquals("zkhost2", uri.getZookeeperQuorum().get(1));
    assertEquals(CassandraFijiURI.DEFAULT_ZOOKEEPER_CLIENT_PORT, uri.getZookeeperClientPort());
    assertEquals("chost", uri.getContactPoints().get(0));
    assertEquals(5678, uri.getContactPort());
    assertEquals("default", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
    assertEquals(null, uri.getUsername());
    assertEquals(null, uri.getPassword());
  }

  @Test
  public void testRelativeToDefaultURI() {
    final CassandraFijiURI uri = CassandraFijiURI.newBuilder("instance/table/col").build();
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
    final CassandraFijiURI uriTwo =
        CassandraFijiURI.newBuilder("///instance///table/////col").build();
    assertEquals("instance", uriTwo.getInstance());
    assertEquals("table", uriTwo.getTable());
    assertEquals("col", uriTwo.getColumns().get(0).getName());
    assertEquals(null, uri.getUsername());
    assertEquals(null, uri.getPassword());
  }

  @Test
  public void testNoAuthority() {
    try {
      CassandraFijiURI.newBuilder("fiji-cassandra:///");
      fail("An exception should have been thrown.");
    } catch (FijiURIException kurie) {
      assertEquals("Invalid Fiji URI: 'fiji-cassandra:///' : ZooKeeper ensemble missing.",
          kurie.getMessage());
    }
  }

  @Test
  public void testBrokenCassandraPort() {
    try {
      CassandraFijiURI.newBuilder("fiji-cassandra://zkhost/chost:port/default").build();
      fail("An exception should have been thrown.");
    } catch (FijiURIException kurie) {
      assertEquals(
          "Invalid Fiji URI: 'fiji-cassandra://zkhost/chost:port/default' "
              + ": Can not parse port 'port'.",
          kurie.getMessage());
    }
  }

  @Test
  public void testMultipleHostsNoParen() {
    try {
      CassandraFijiURI.newBuilder(
          "fiji-cassandra://zkhost1,zkhost2:1234/chost:5678/instance/table/col");
      fail("An exception should have been thrown.");
    } catch (FijiURIException kurie) {
      assertEquals(
          "Invalid Fiji URI: 'fiji-cassandra://zkhost1,zkhost2:1234/chost:5678/instance/table/col'"
              + " : Multiple ZooKeeper hosts must be parenthesized.", kurie.getMessage());
    }
  }

  @Test
  public void testMultipleHostsMultiplePorts() {
    try {
      CassandraFijiURI.newBuilder(
          "fiji-cassandra://zkhost1:1234,zkhost2:2345/chost:5678/instance/table/col");
      fail("An exception should have been thrown.");
    } catch (FijiURIException kurie) {
      assertEquals(
          "Invalid Fiji URI: 'fiji-cassandra://zkhost1:1234,zkhost2:2345/chost:5678/"
              + "instance/table/col' : Invalid ZooKeeper ensemble cluster identifier.",
          kurie.getMessage());
    }
  }

  @Test
  public void testMultipleColumns() {
    final CassandraFijiURI uri =
        CassandraFijiURI.newBuilder(
            "fiji-cassandra://zkhost1,zkhost2/chost:5678/default/table/col1,col2").build();
    assertEquals("zkhost1", uri.getZookeeperQuorum().get(0));
    assertEquals("zkhost2", uri.getZookeeperQuorum().get(1));
    assertEquals(CassandraFijiURI.DEFAULT_ZOOKEEPER_CLIENT_PORT, uri.getZookeeperClientPort());
    assertEquals("default", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals(2, uri.getColumns().size());
    assertEquals(null, uri.getUsername());
    assertEquals(null, uri.getPassword());
  }

  @Test
  public void testExtraPath() {
    try {
      CassandraFijiURI.newBuilder(
          "fiji-cassandra://(zkhost1,zkhost2):1234/chost:5678/instance/table/col/extra");
      fail("An exception should have been thrown.");
    } catch (FijiURIException kurie) {
      assertEquals(
          "Invalid Fiji URI: 'fiji-cassandra://(zkhost1,zkhost2):1234/chost:5678/"
              + "instance/table/col/extra' : Too many path segments.",
          kurie.getMessage());
    }
  }

  @Test
  public void testURIWithQuery() {
    final CassandraFijiURI uri =
        CassandraFijiURI.newBuilder(
            "fiji-cassandra://(zkhost1,zkhost2):1234/chost:5678/instance/table/col?query").build();
    assertEquals("zkhost1", uri.getZookeeperQuorum().get(0));
    assertEquals("zkhost2", uri.getZookeeperQuorum().get(1));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
    assertEquals(null, uri.getUsername());
    assertEquals(null, uri.getPassword());
  }

  @Test
  public void testURIWithFragment() {
    final CassandraFijiURI uri =
        CassandraFijiURI.newBuilder(
            "fiji-cassandra://(zkhost1,zkhost2):1234/chost:5678/instance/table/col#frag").build();
    assertEquals("zkhost1", uri.getZookeeperQuorum().get(0));
    assertEquals("zkhost2", uri.getZookeeperQuorum().get(1));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
    assertEquals(null, uri.getUsername());
    assertEquals(null, uri.getPassword());
  }

  @Test
  public void testPartialURIZookeeper() {
    final CassandraFijiURI uri =
        CassandraFijiURI.newBuilder("fiji-cassandra://zkhost:1234/chost:5678").build();
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals(null, uri.getInstance());
    assertEquals(null, uri.getUsername());
    assertEquals(null, uri.getPassword());
  }

  @Test
  public void testBasicResolution() {
    final CassandraFijiURI uri =
        CassandraFijiURI.newBuilder("fiji-cassandra://zkhost:1234/chost:5678").build();
    final CassandraFijiURI resolved = uri.resolve("testinstance");
    assertEquals("testinstance", resolved.getInstance());
  }

  @Test
  public void testResolution() {
    final CassandraFijiURI uri =
        CassandraFijiURI.newBuilder("fiji-cassandra://zkhost:1234/chost:5678/.unset").build();
    final CassandraFijiURI resolved = uri.resolve("testinstance");
    assertEquals("testinstance", resolved.getInstance());
  }

  @Test
  public void testResolutionColumn() {
    final CassandraFijiURI uri =
        CassandraFijiURI.newBuilder("fiji-cassandra://zkhost/chost:5678/instance/table").build();
    final CassandraFijiURI resolved = uri.resolve("col");
    assertEquals("col", resolved.getColumns().get(0).getName());
  }

  @Test
  public void testInvalidResolution() {
    final CassandraFijiURI uri =
        CassandraFijiURI.newBuilder("fiji-cassandra://zkhost:1234/chost:5678").build();
    try {
      uri.resolve("instance/table/col/extra");
      fail("An exception should have been thrown.");
    } catch (FijiURIException kurie) {
      assertEquals("Invalid Fiji URI: "
          + "'fiji-cassandra://zkhost:1234/chost:5678/instance/table/col/extra' : "
              + "Too many path segments.", kurie.getMessage());
    }
  }

  @Test
  public void testToString() {
    String uri = "fiji-cassandra://(zkhost1,zkhost2):1234/chost:5678/instance/table/col/";
    assertEquals(uri, CassandraFijiURI.newBuilder(uri).build().toString());
    uri = "fiji-cassandra://zkhost1:1234/chost:5678/instance/table/col/";
    assertEquals(uri, CassandraFijiURI.newBuilder(uri).build().toString());
    uri = "fiji-cassandra://zkhost:1234/chost:5678/instance/table/col1,col2/";
    assertEquals(uri, CassandraFijiURI.newBuilder(uri).build().toString());
    uri = "fiji-cassandra://zkhost:1234/chost:5678/.unset/table/col/";
    assertEquals(uri, CassandraFijiURI.newBuilder(uri).build().toString());
    uri = "fiji-cassandra://(zkhost1,zkhost2):1234/user:pass@chost:5678/instance/table/col/";
    assertEquals(uri, CassandraFijiURI.newBuilder(uri).build().toString());
  }

  @Test
  public void testNormalizedQuorum() {
    CassandraFijiURI uri = CassandraFijiURI.newBuilder(
        "fiji-cassandra://(zkhost1,zkhost2):1234/chost:5678/instance/table/col/").build();
    CassandraFijiURI reversedQuorumUri = CassandraFijiURI.newBuilder(
        "fiji-cassandra://(zkhost2,zkhost1):1234/chost:5678/instance/table/col/").build();
    assertEquals(uri.toString(), reversedQuorumUri.toString());
    assertEquals(uri.getZookeeperQuorum(), reversedQuorumUri.getZookeeperQuorum());
  }

  @Test
  public void testNormalizedCassandraNodes() {
    CassandraFijiURI uri = CassandraFijiURI.newBuilder(
        "fiji-cassandra://zkhost:1234/(chost1,chost2):5678/instance/table/col/").build();
    CassandraFijiURI reversedUri = CassandraFijiURI.newBuilder(
        "fiji-cassandra://zkhost:1234/(chost2,chost1):5678/instance/table/col/").build();
    assertEquals(uri.toString(), reversedUri.toString());
    assertEquals(uri.getContactPoints(), reversedUri.getContactPoints());
  }

  @Test
  public void testNormalizedColumns() {
    CassandraFijiURI uri = CassandraFijiURI.newBuilder(
        "fiji-cassandra://(zkhost1,zkhost2):1234/chost:5678/instance/table/col/").build();
    CassandraFijiURI reversedColumnURI = CassandraFijiURI.newBuilder(
        "fiji-cassandra://(zkhost2,zkhost1):1234/chost:5678/instance/table/col/").build();
    assertEquals(uri.toString(), reversedColumnURI.toString());
    assertEquals(uri.getColumns(), reversedColumnURI.getColumns());
  }

  @Test
  public void testOrderedQuorum() {
    CassandraFijiURI uri = CassandraFijiURI.newBuilder(
        "fiji-cassandra://(zkhost1,zkhost2):1234/chost:5678/instance/table/col/").build();
    CassandraFijiURI reversedQuorumUri = CassandraFijiURI.newBuilder(
        "fiji-cassandra://(zkhost2,zkhost1):1234/chost:5678/instance/table/col/").build();
    assertFalse(uri.getZookeeperQuorumOrdered()
        .equals(reversedQuorumUri.getZookeeperQuorumOrdered()));
    assertFalse(uri.toOrderedString().equals(reversedQuorumUri.toOrderedString()));
  }

  @Test
  public void testOrderedCassandraNodes() {
    String revString =  "fiji-cassandra://zkhost:1234/(chost2,chost1):5678/instance/table/col/";
    String ordString =  "fiji-cassandra://zkhost:1234/(chost1,chost2):5678/instance/table/col/";
    CassandraFijiURI revUri = CassandraFijiURI.newBuilder(revString).build();

    // "toString" should ignore the user-defined ordering.
    assertEquals(ordString, revUri.toString());

    // "toOrderedString" should maintain the user-defined ordering.

  }
  @Test
  public void testOrderedColumns() {
    CassandraFijiURI uri = CassandraFijiURI.newBuilder(
        "fiji-cassandra://(zkhost1,zkhost2):1234/chost:5678/instance/table/col1,col2/").build();
    CassandraFijiURI reversedColumnURI = CassandraFijiURI.newBuilder(
        "fiji-cassandra://(zkhost1,zkhost2):1234/chost:5678/instance/table/col2,col1/").build();
    assertFalse(uri.toOrderedString().equals(reversedColumnURI.toOrderedString()));
    assertFalse(uri.getColumnsOrdered().equals(reversedColumnURI.getColumnsOrdered()));
  }

  /**
   * Tests that CassandraFijiURI.newBuilder().build() builds a URI for the default Fiji instance
   * URI.
   *
   * The default Fiji instance URI is environment specific. Hence, this cannot test for explicit
   * values of the ZooKeeper quorum of of the ZooKeeper client port.
   */
  @Test
  public void testCassandraFijiURIBuilderDefault() {
    CassandraFijiURI uri = CassandraFijiURI.newBuilder().build();
    assertTrue(!uri.getZookeeperQuorum().isEmpty());  // Test cannot be more specific.
    // Test cannot validate the value of uri.getZookeeperClientPort().
    assertEquals(KConstants.DEFAULT_INSTANCE_NAME, uri.getInstance());
    assertEquals(null, uri.getTable());
    assertTrue(uri.getColumns().isEmpty());
  }

  @Test
  public void testCassandraFijiURIBuilderFromInstance() {
    final CassandraFijiURI uri = CassandraFijiURI.newBuilder(
        "fiji-cassandra://zkhost:1234/chost:5678/.unset/table").build();
    CassandraFijiURI built = CassandraFijiURI.newBuilder(uri).build();
    assertEquals(uri, built);
  }

  @Test
  public void testCassandraFijiURIBuilderWithInstance() {
    final CassandraFijiURI uri = CassandraFijiURI.newBuilder(
        "fiji-cassandra://zkhost:1234/chost:5678/instance1/table").build();
    assertEquals("instance1", uri.getInstance());
    final CassandraFijiURI modified =
        CassandraFijiURI.newBuilder(uri).withInstanceName("instance2").build();
    assertEquals("instance2", modified.getInstance());
    assertEquals("instance1", uri.getInstance());
  }

  @Test
  public void testSetColumn() {
    CassandraFijiURI uri = CassandraFijiURI.newBuilder(
        "fiji-cassandra://zkhost/chost:5678/instance/table/").build();
    assertTrue(uri.getColumns().isEmpty());
    uri =
        CassandraFijiURI.newBuilder(uri).withColumnNames(Arrays.asList("testcol1", "testcol2"))
        .build();
    assertEquals(2, uri.getColumns().size());
  }

  @Test
  public void testSetZookeeperQuorum() {
    final CassandraFijiURI uri = CassandraFijiURI.newBuilder(
        "fiji-cassandra://zkhost/chost:5678/instance/table/col").build();
    final CassandraFijiURI modified = CassandraFijiURI.newBuilder(uri)
        .withZookeeperQuorum(new String[] {"zkhost1", "zkhost2"}).build();
    assertEquals(2, modified.getZookeeperQuorum().size());
    assertEquals("zkhost1", modified.getZookeeperQuorum().get(0));
    assertEquals("zkhost2", modified.getZookeeperQuorum().get(1));
  }

  @Test
  public void testTrailingUnset() {
    final CassandraFijiURI uri = CassandraFijiURI.newBuilder(
        "fiji-cassandra://zkhost/chost:5678/.unset/table/.unset").build();
    CassandraFijiURI result = CassandraFijiURI.newBuilder(uri).withTableName(".unset").build();
    assertEquals("fiji-cassandra://zkhost:2181/chost:5678/", result.toString());
  }

  @Test
  public void testEscapedMapColumnQualifier() {
    final CassandraFijiURI uri = CassandraFijiURI.newBuilder(
        "fiji-cassandra://zkhost/chost:5678/instance/table/map:one%20two").build();
    assertEquals("map:one two", uri.getColumns().get(0).getName());
  }

  @Test
  public void testConstructedUriIsEscaped() {
    // SCHEMA-6. Column qualifier must be URL-encoded in CassandraFijiURI.
    final CassandraFijiURI uri =
        CassandraFijiURI.newBuilder("fiji-cassandra://zkhost/chost:5678/instance/table/")
        .addColumnName(new FijiColumnName("map:one two")).build();
    assertEquals(
        "fiji-cassandra://zkhost:2181/chost:5678/instance/table/map:one%20two/", uri.toString());
  }
}
