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

package com.moz.fiji.rest.plugins;

import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import com.moz.fiji.rest.FijiClient;
import com.moz.fiji.rest.FijiRESTConfiguration;

/**
 * Interface for plugins which will add custom resources to the Dropwizard environment.
 */
public interface FijiRestPlugin {

  /**
   * For every accessible plugin, this method is invoked when FijiRESTService initializes.
   * Bootstraps the plugin.  Often useful for adding asset bundles.
   *
   * @param bootstrap the service bootstrap
   */
  void initialize(final Bootstrap<FijiRESTConfiguration> bootstrap);

  /**
   * For every accessible plugin, this method is invoked when FijiRESTService initializes.
   * Creates and adds resources to the environment.
   *
   * @param fijiClient that this should use for connecting to Fiji.
   * @param configuration of the FijiREST setup.
   * @param environment of Dropwizard.
   */
  void install(
      final FijiClient fijiClient,
      final FijiRESTConfiguration configuration,
      final Environment environment);
}
