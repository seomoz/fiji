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

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;

/** Runtime exception thrown when encoding a cell's content fails. */
@ApiAudience.Public
@ApiStability.Stable
public final class FijiEncodingException extends RuntimeException {

  /**
   * Initializes an encoding exception.
   *
   * @param message Error message.
   */
  public FijiEncodingException(String message) {
    super(message);
  }

  /**
   * Initializes an encoding exception.
   *
   * @param message Error message.
   * @param throwable Underlying exception, if any.
   */
  public FijiEncodingException(String message, Throwable throwable) {
    super(message, throwable);
  }

  /**
   * Initializes an encoding exception.
   *
   * @param throwable Underlying exception, if any.
   */
  public FijiEncodingException(Throwable throwable) {
    super(throwable);
  }
}
