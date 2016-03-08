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

import io.dropwizard.jersey.setup.JerseyEnvironment;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import com.moz.fiji.rest.FijiClient;
import com.moz.fiji.rest.FijiRESTConfiguration;
import com.moz.fiji.rest.resources.InstanceResource;
import com.moz.fiji.rest.resources.InstancesResource;
import com.moz.fiji.rest.resources.FijiRESTResource;
import com.moz.fiji.rest.resources.RowsResource;
import com.moz.fiji.rest.resources.TableResource;
import com.moz.fiji.rest.resources.TablesResource;

/**
 * Installs default FijiREST endpoints into the Dropwizard environment.
 */
public class StandardFijiRestPlugin implements FijiRestPlugin {
  /** {@inheritDoc} */
  @Override
  public void initialize(Bootstrap<FijiRESTConfiguration> bootstrap) {}

  /** {@inheritDoc} */
  @Override
  public void install(
      final FijiClient fijiClient,
      final FijiRESTConfiguration configuration,
      final Environment environment
  ) {

    final JerseyEnvironment jersey = environment.jersey();
    // Adds resources.
    jersey.register(new FijiRESTResource());
    jersey.register(new InstancesResource(fijiClient));
    jersey.register(new InstanceResource(fijiClient));
    jersey.register(new TableResource(fijiClient));
    jersey.register(new TablesResource(fijiClient));
    jersey.register(new RowsResource(fijiClient, environment.getObjectMapper()));
  }
}
