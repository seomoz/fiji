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

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import javax.ws.rs.ext.ExceptionMapper;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.delegation.Lookups;
import com.moz.fiji.rest.health.FijiClientHealthCheck;
import com.moz.fiji.rest.plugins.FijiRestPlugin;
import com.moz.fiji.rest.representations.FijiRestEntityId;
import com.moz.fiji.rest.representations.SchemaOption;
import com.moz.fiji.rest.serializers.AvroToJsonStringSerializer;
import com.moz.fiji.rest.serializers.JsonToFijiRestEntityId;
import com.moz.fiji.rest.serializers.JsonToSchemaOption;
import com.moz.fiji.rest.serializers.FijiRestEntityIdToJson;
import com.moz.fiji.rest.serializers.SchemaOptionToJson;
import com.moz.fiji.rest.serializers.TableLayoutToJsonSerializer;
import com.moz.fiji.rest.serializers.Utf8ToJsonSerializer;
import com.moz.fiji.rest.tasks.CloseTask;
import com.moz.fiji.rest.tasks.RefreshInstancesTask;
import com.moz.fiji.rest.tasks.ShutdownTask;
import com.moz.fiji.schema.FijiURI;

/**
 * Service to provide REST access to a list of Fiji instances.
 * The configuration is parametrized by a file that contains the cluster
 * address and the list of instances.
 */
public class FijiRESTService extends Application<FijiRESTConfiguration> {

  private static final Logger LOG = LoggerFactory.getLogger(FijiRESTService.class);

  /**
   * Main method entry point into the FijiREST service.
   *
   * @param args The server token and the path to the YAML configuration file.
   * @throws Exception Prevents the REST service from starting.
   */
  public static void main(final String[] args) throws Exception {
    new FijiRESTService().run(args);
  }

  /** {@inheritDoc} */
  @Override
  public final void initialize(final Bootstrap<FijiRESTConfiguration> bootstrap) {
    // Initialize plugins.
    for (FijiRestPlugin plugin : Lookups.get(FijiRestPlugin.class)) {
      LOG.info("Initializing plugin {}", plugin.getClass());
      plugin.initialize(bootstrap);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @throws IOException when instance in configuration can not be opened and closed.
   */
  @Override
  public final void run(
      final FijiRESTConfiguration configuration,
      final Environment environment
  ) throws IOException {
    final FijiURI clusterURI = FijiURI.newBuilder(configuration.getClusterURI()).build();
    final ManagedFijiClient managedFijiClient = new ManagedFijiClient(configuration);
    environment.lifecycle().manage(managedFijiClient);

    // Setup the health checker for the FijiClient
    environment
        .healthChecks()
        .register("FijiClientHealthCheck", new FijiClientHealthCheck(managedFijiClient));

    // Remove all built-in Dropwizard ExceptionHandler.
    // Always depend on custom ones.
    // Inspired by Jeremy Whitlock's suggestion on thoughtspark.org.
    Set<Object> jerseyResources = environment.jersey().getResourceConfig().getSingletons();
    Iterator<Object> jerseyResourcesIterator = jerseyResources.iterator();
    while (jerseyResourcesIterator.hasNext()) {
      Object jerseyResource = jerseyResourcesIterator.next();
      if (jerseyResource instanceof ExceptionMapper
          && jerseyResource.getClass().getName().startsWith("io.dropwizard.jersey")) {
        jerseyResourcesIterator.remove();
      }
    }

    // Load admin task to manually refresh instances.
    environment.admin().addTask(new RefreshInstancesTask(managedFijiClient));
    // Load admin task to manually close instances and tables.
    environment.admin().addTask(new CloseTask(managedFijiClient));
    // Load admin task to manually shutdown the system.
    environment.admin().addTask(new ShutdownTask(managedFijiClient, configuration));

    // Adds custom serializers.
    registerSerializers(environment.getObjectMapper());

    // Adds exception mappers to print better exception messages to the client than what
    // Dropwizard does by default.
    environment.jersey().register(new GeneralExceptionMapper());

    // Load resources.
    for (FijiRestPlugin plugin : Lookups.get(FijiRestPlugin.class)) {
      LOG.info("Loading plugin {}", plugin.getClass());
      plugin.install(managedFijiClient, configuration, environment);
    }

    // Allow global CORS filter. CORS off by default.
    if (configuration.getCORS()) {
      environment.servlets().addFilter("CrossOriginFilter", CrossOriginFilter.class);
      LOG.info("Global cross-origin resource sharing is allowed.");
    }
  }

  /**
   * Registers custom serializers with the Jackson ObjectMapper. This is used by both the service
   * initialization and the test setup method to ensure consistency between test and production.
   *
   * @param objectMapper is the ObjectMapper.
   */
  public static void registerSerializers(ObjectMapper objectMapper) {
    // TODO: Add a module to convert btw Avro's specific types and JSON. The default
    // mapping seems to throw an exception.
    SimpleModule module =
        new SimpleModule(
            "FijiRestModule",
            new Version(1, 0, 0, null, "com.moz.fiji.rest", "serializers"));
    module.addSerializer(new AvroToJsonStringSerializer());
    module.addSerializer(new Utf8ToJsonSerializer());
    module.addSerializer(new TableLayoutToJsonSerializer());
    module.addSerializer(new SchemaOptionToJson());
    module.addDeserializer(SchemaOption.class, new JsonToSchemaOption());
    module.addSerializer(new FijiRestEntityIdToJson());
    module.addDeserializer(FijiRestEntityId.class, new JsonToFijiRestEntityId());
    objectMapper.registerModule(module);
  }
}
