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

package com.moz.fiji.rest.health;

import com.codahale.metrics.health.HealthCheck;

import com.moz.fiji.rest.ManagedFijiClient;

/**
 * A HealthCheck for checking the health of a ManagedFijiClient.
 */
public class FijiClientHealthCheck extends HealthCheck {

  private final ManagedFijiClient mFijiClient;

  /**
   * Create a HealthCheck instance for the supplied ManagedFijiClient.
   *
   * @param fijiClient to check health of.
   */
  public FijiClientHealthCheck(ManagedFijiClient fijiClient) {
    this.mFijiClient = fijiClient;
  }

  /** {@inheritDoc} */
  @Override
  protected Result check() throws Exception {
   return mFijiClient.checkHealth();
  }
}
