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

package com.moz.fiji.rest.tasks;

import java.io.PrintWriter;
import java.util.Collection;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableMultimap;
import io.dropwizard.servlets.tasks.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.rest.FijiRESTConfiguration;
import com.moz.fiji.rest.ManagedFijiClient;

/**
 * This REST task allows administrators to shutdown the system.
 */
public class ShutdownTask extends Task {
  private static final Logger LOG = LoggerFactory.getLogger(ShutdownTask.class);

  private final FijiRESTConfiguration mFijiRESTConfiguration;
  private final ManagedFijiClient mManagedFijiClient;

  /**
   * Create a ShutdownTask with the provided FijiClient.
   *
   * @param managedFijiClient the client that will be gracefully shut down.
   * @param configuration to use to examine if shutdown is available.
   */
  public ShutdownTask(ManagedFijiClient managedFijiClient, FijiRESTConfiguration configuration) {
    super("shutdown");
    mManagedFijiClient = managedFijiClient;
    mFijiRESTConfiguration = configuration;
  }

  /** {@inheritDoc} */
  @Override
  public void execute(
      ImmutableMultimap<String, String> parameters,
      PrintWriter output
  ) throws Exception {
    final Collection<String> commands = parameters.get("force");

    if (mFijiRESTConfiguration.isShutdownEnabled()) {
      // Presence of "force" parameter will trigger a forced shutdown.
      if (commands.size() == 0) {
        mManagedFijiClient.stop();
        output.println("Server has been shutdown");
        output.flush();
      }
      System.exit(0);
    } else {
      throw new WebApplicationException(
          new IllegalArgumentException("Remote shutdown is disabled. Enable by setting the "
              + "remote-shutdown command to true in the configuration file."),
          Response.Status.BAD_REQUEST);
    }
  }
}
