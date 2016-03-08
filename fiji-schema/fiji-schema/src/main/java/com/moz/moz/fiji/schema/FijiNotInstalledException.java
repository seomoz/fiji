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

package com.moz.fiji.schema;

import com.google.common.base.Preconditions;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;

/** Thrown when attempting to open a non existing/not installed Fiji instance. */
@ApiAudience.Public
@ApiStability.Stable
public final class FijiNotInstalledException extends RuntimeException {
  /** The instance name of the missing Fiji instance. */
  private final String mInstanceName;

  /** The URI of the missing Fiji instance. */
  private final FijiURI mURI;

  /**
   * Creates a new <code>FijiNotInstalledException</code> with the specified
   * detail message.
   *
   * @param message The exception message.
   * @param fijiURI The URI for the uninstalled instance.
   */
   public FijiNotInstalledException(String message, FijiURI fijiURI) {
    super(message);
    mURI = fijiURI;
    mInstanceName = null;
  }

  /**
   * Returns the URI of the missing Fiji instance.
   * @return the URI of the missing Fiji instance.
   */
  public FijiURI getURI() {
    return mURI;
  }

  /**
   * Creates a new <code>FijiNotInstalledException</code> with the specified
   * detail message.
   *
   * @param message The exception message.
   * @param instanceName The Fiji instance name that is not installed.
   * @deprecated Use {@link FijiNotInstalledException#FijiNotInstalledException(String, String)}.
   */
  public FijiNotInstalledException(String message, String instanceName) {
    super(message);
    mInstanceName = instanceName;
    mURI = null;
  }

  /**
   * Returns the name of the missing Fiji instance.
   * @return the name of the missing Fiji instance.
   */
  public String getInstanceName() {
    if (mURI == null) {
      Preconditions.checkNotNull(mInstanceName != null);
      return mInstanceName;
    } else {
      return mURI.getInstance();
    }
  }
}
