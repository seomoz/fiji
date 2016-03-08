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

package com.moz.fiji.schema.security;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;

/**
 * Thrown when an error occurs with the configuration or internal operation of Fiji security.
 * Errors encountered due to a user's lack of permission throw {@link FijiAccessException}.
 */
@ApiAudience.Public
@ApiStability.Stable
public final class FijiSecurityException extends RuntimeException {
  /**
   * Creates a new <code>FijiSecurityException</code> with the specified detail message.
   *
   * @param message The exception message.
   * @param cause The cause of the exception, which may contain additional information.
   */
  public FijiSecurityException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates a new <code>FijiSecurityException</code> with the specified detail message.
   *
   * @param message The exception message.
   */
  public FijiSecurityException(String message) {
    super(message);
  }
}
