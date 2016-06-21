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

package com.moz.fiji.schema.tools;

import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.common.flags.Flag;
import com.moz.fiji.schema.KConstants;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.zookeeper.ZooKeeperUtils;

/**
 * A command-line tool for listing and discovering Fiji services.
 *
 * <H2>Usage</H2>
 *
 * List all Fiji services and their instance counts:
 * <code><pre>
 *   fiji services
 * </pre></code>
 *
 * List instances of a specific Fiji service:
 * <code><pre>
 *   fiji services fiji-rest
 * </pre></code>
 *
 * List all instances of all Fiji services:
 * <code><pre>
 *   fiji services fiji-rest --all
 * </pre></code>
 */
@ApiAudience.Private
public final class ServicesTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(ServicesTool.class);
  private static final String BASE_PATH = "/fiji/services";

  @Flag(name="fiji", usage="URI of the Fiji cluster to use for service discovery.")
  private String mFijiURIFlag = KConstants.DEFAULT_INSTANCE_URI;

  @Flag(name="zookeeper-ensemble",
      usage="The ZooKeeper ensemble to use for service discovery."
          + " May not be specified if the 'fiji' flag is specified.")
  private String mZKEnsembleFlag;

  @Flag(name="all", usage="List all instances of every service.")
  private boolean mAll = false;

  /** {@inheritDoc} */
  @Override
  public String getUsageString() {
    return
        "Usage:\n"
            + "    fiji services [flags...] [<service>...] \n"
            + "\n"
            + "Example:\n"
            + "  List the Fiji services from the default cluster:\n"
            + "    fiji services\n"
            + "    fiji services --fiji=fiji://.env\n"
            + "\n"
            + "  List the Fiji services from a ZooKeeper ensemble:\n"
            + "    fiji services --zookeeper-ensemble=host1:2181,host2:2181\n"
            + "\n"
            + "  List the instances of the Fiji service named 'fiji-rest':\n"
            + "    fiji services fiji-rest\n"
            + "    fiji services --fiji=fiji://.env/default fiji-rest\n"
            + "\n"
            + "  List the instance of every Fiji services:\n"
            + "    fiji services --all\n";
  }

  /** ZooKeeper ensemble to connect to. */
  private String mZKEnsemble;

  /** {@inheritDoc} */
  @Override
  protected void setup() throws Exception {
    super.setup();
    boolean fijiURISpecified = mFijiURIFlag != null && !mFijiURIFlag.isEmpty();
    boolean zkEnsembleSpecified = mZKEnsembleFlag != null && !mZKEnsembleFlag.isEmpty();

    Preconditions.checkArgument(fijiURISpecified || zkEnsembleSpecified,
        "Specify the Fiji cluster URI or ZooKeeper ensemble to use for service discovery.");

    Preconditions.checkArgument(
        !(fijiURISpecified && zkEnsembleSpecified)
            || mFijiURIFlag.equals(KConstants.DEFAULT_INSTANCE_URI),
        "Specify either the Fiji cluster URI or the ZooKeeper ensemble.");

    if (zkEnsembleSpecified) {
      mZKEnsemble = mZKEnsembleFlag;
    } else if (fijiURISpecified) {
      mZKEnsemble = FijiURI.newBuilder(mFijiURIFlag).build().getZooKeeperEnsemble();
    } else {
      throw new IllegalArgumentException(
          // This can't actually happen since the default fiji is used by default.
          "Must specify a ZooKeeper ensemble with --zookeeper-ensemble=host1:2181,host2:2181"
              + " or a Fiji cluster URI with --fiji=fiji://(host1,host2):2181/");
    }
  }

  @Override
  protected int run(List<String> instanceLookups) throws Exception {
    // NOTE: this is not the 'recommended' way to use service discovery. Typically, an application
    // needs to discover services of a known type, at which point it could use the Curator discovery
    // framework to automatically discover registered services and deserialize payloads. Because
    // this tool is generic to all service types (and thus can't know how to deserialize the
    // payload), it's easier just to manually retrieve each service through the normal ZK means.
    // Additionally, this means we don't have to rely on curator-x-discovery in fiji schema.

    final CuratorFramework zkClient = ZooKeeperUtils.getZooKeeperClient(mZKEnsemble);
    try {
      getPrintStream().println("Fiji Services:");

      if (mAll) {
        instanceLookups = getServices(zkClient);
      }

      if (instanceLookups.isEmpty()) {
        // List available services:
        for (String service : getServices(zkClient)) {
          final List<String> instances = getInstanceIDs(zkClient, service);
          getPrintStream().println(String.format("\t%s (%s instances)", service, instances.size()));
        }
      } else {
        // List instances of specific services
        for (String service : instanceLookups) {
          getPrintStream().println(String.format("\t%s", service));
          for (Map.Entry<String, String> entry : getInstances(zkClient, service).entrySet()) {
            getPrintStream().println(
                String.format("\t\t%s %s", entry.getKey(), entry.getValue()));
          }
        }
      }
    } finally {
      zkClient.close();
    }

    return 0;
  }

  /**
   * Retrieve the services in ZooKeeper. They may not have any registered instances.
   *
   * @param zkClient connection to ZooKeeper.
   * @return list of services.
   * @throws Exception on unrecoverable ZooKeeper error.
   */
  private static List<String> getServices(final CuratorFramework zkClient) throws Exception {
    try {
      return zkClient.getChildren().forPath(BASE_PATH);
    } catch (NoNodeException nne) {
      // No services
      return ImmutableList.of();
    }
  }

  /**
   * Retrieve the IDs of the instances of a Fiji service.
   *
   * @param zkClient to connect to ZooKeeper with.
   * @param service name to get instances of.
   * @return the IDs of the instances of the Fiji service.
   * @throws Exception on unrecoverable ZooKeeper error.
   */
  private static List<String> getInstanceIDs(
      final CuratorFramework zkClient,
      final String service
  ) throws Exception {

    try {
      return zkClient.getChildren().forPath(String.format("%s/%s", BASE_PATH, service));
    } catch (NoNodeException nne) {
      // The service does not exist; no instances.
      return ImmutableList.of();
    }
  }

  /**
   * Retrieve the instances of a Fiji service.  The returned map is from ID to payload.
   *
   * @param zkClient to connect to ZooKeeper with.
   * @param service name to get instances of.
   * @return map of instance ID to payload.
   * @throws Exception on unrecoverable ZooKeeper error.
   */
  private static Map<String, String> getInstances(
      final CuratorFramework zkClient,
      final String service
  ) throws Exception {
    final List<String> instanceIDs = getInstanceIDs(zkClient, service);
    final Map<String, String> instances = Maps.newHashMap();
    final String servicePath = String.format("%s/%s/", BASE_PATH, service);
    for (String id : instanceIDs) {
      try {
        final byte[] payload = zkClient.getData().forPath(servicePath + id);
        instances.put(id, Bytes.toStringBinary(payload));
      } catch (NoNodeException nne) {
        // Service stopped
      }
    }
    return instances;
  }

  @Override
  public String getName() {
    return "services";
  }

  @Override
  public String getDescription() {
    return "Discover registered Fiji services.";
  }

  @Override
  public String getCategory() {
    return "Admin";
  }
}
