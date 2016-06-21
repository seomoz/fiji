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

package com.moz.fiji.rest.tasks;

import java.io.PrintWriter;

import com.google.common.collect.ImmutableMultimap;
import io.dropwizard.servlets.tasks.Task;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.rest.ManagedFijiClient;

/**
 * This REST resource interacts with Fiji instances collection resource.
 *
 * This resource is served for requests using the resource identifier:
 * <ul>
 * <li>/v1/instances/</li>
 * </ul>
 */
@ApiAudience.Public
public class RefreshInstancesTask extends Task {
  private final ManagedFijiClient mFijiClient;

  /**
   * Creates a task which manually refreshes the instances served by the provided ManagedFijiClient.
   *
   * @param fijiClient to be refreshed.
   */
  public RefreshInstancesTask(ManagedFijiClient fijiClient) {
    super("refresh_instances");
    this.mFijiClient = fijiClient;
  }

  @Override
  public void execute(ImmutableMultimap<String, String> arg0, PrintWriter arg1)
      throws Exception {
    mFijiClient.refreshInstances();
    arg1.println("Manually refreshed instances...");
    arg1.flush();
  }
}
