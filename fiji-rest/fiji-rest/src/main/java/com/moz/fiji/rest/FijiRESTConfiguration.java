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

package com.moz.fiji.rest;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import org.hibernate.validator.constraints.NotEmpty;

/**
 * The Java object which is deserialized from the YAML configuration file.
 * This parametrizes the FijiRESTService.
 */
public class FijiRESTConfiguration extends Configuration {
  /** String cluster address. */
  @NotEmpty
  @JsonProperty("cluster")
  private String mCluster;

  /** Set cache timeout in minutes. */
  @JsonProperty("cacheTimeout")
  private long mCacheTimeout = 10;

  /** Subconfiguration controlling the visibility of instances. */
  @JsonProperty("instances")
  private Set<String> mInstances;

  /** Set global CORS support. */
  @JsonProperty("cors")
  private boolean mCORS = false;

  /** Set support for admin REST shutdown commands. */
  @JsonProperty("remote-shutdown")
  private boolean mShutdownEnabled = true;

  /** For plugins to add arbitary properties. */
  @JsonProperty("plugin-properties")
  private Map<String, String> mPluginProperties = Collections.emptyMap();

  /** Whether to register this REST server with service discovery. */
  @JsonProperty("service-discovery")
  private boolean mServiceDiscovery = true;

  /** @return The cluster address. */
  public final String getClusterURI() {
    return mCluster;
  }

  /** @return The caching timeout. */
  public final long getCacheTimeout() {
    return mCacheTimeout;
  }

  /** @return The set of visible instances. */
  public Set<String> getVisibleInstances() {
    return mInstances;
  }

  /** @return Is global CORS turned on or off. */
  public boolean getCORS() {
    return mCORS;
  }

  /** @return Is remote shutdown via REST enabled. */
  public boolean isShutdownEnabled() {
    return mShutdownEnabled;
  }

  /** @return register this REST instance with service discovery. */
  public boolean getServiceDiscovery() {
    return mServiceDiscovery;
  }

  /**
   * @return A map of arbitrary properties for use by plugins.
   */
  public Map<String, String> getPluginProperties() {
    return mPluginProperties;
  }
}
