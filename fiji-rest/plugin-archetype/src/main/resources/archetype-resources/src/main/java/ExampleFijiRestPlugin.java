#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
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

package ${package};

import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import com.moz.fiji.rest.GeneralExceptionMapper;
import com.moz.fiji.rest.FijiClient;
import com.moz.fiji.rest.FijiRESTConfiguration;
import com.moz.fiji.rest.plugins.FijiRestPlugin;

  /**
   * Installs default FijiREST endpoints into the Dropwizard environment.
   */
  public class ExampleFijiRestPlugin implements FijiRestPlugin {

    /** {@inheritDoc} */
    @Override
    public void initialize(Bootstrap<FijiRESTConfiguration> bootstrap) {
      // To include static assets put them in src/main/resources/assets and uncomment the
      // following line:
      //bootstrap.addBundle(new AssetsBundle());
    }

    /** {@inheritDoc} */
    @Override
    public void install(
        final FijiClient fijiClient,
        final FijiRESTConfiguration configuration,
        final Environment environment) {

      // Adds resources.
      environment.addResource(new ExampleResource(fijiClient));
    }
  }
