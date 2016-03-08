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

/**
 * Runtime (unchecked) IOException.
 *
 * <p> Use this exception when you cannot throw IOException directly (eg. because checked
 *     exceptions do not allow IOException).
 *
 * <p> For example, FijiPager.next() cannot throw IOException because it implements interfaces
 *     Iterator. In this case, an underlying IOException may be wrapped as a FijiIOException.
 */
@ApiAudience.Public
@ApiStability.Stable
@SuppressWarnings("serial")
public final class FijiIOException extends RuntimeException {

  /** Creates an unchecked IOException. */
  public FijiIOException() {
  }

  /**
   * Creates an unchecked IOException with the specified message and cause.
   *
   * @param message Message to include in this IOException.
   * @param cause Underlying cause of the IOException.
   */
  public FijiIOException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates an unchecked IOException with the specified message.
   *
   * @param message Message to include in the IOException.
   */
  public FijiIOException(String message) {
    super(message);
  }

  /**
   * Wraps an IOException into an unchecked exception.
   *
   * @param cause Underlying cause of the IOException.
   */
  public FijiIOException(Throwable cause) {
    super(cause);
  }
}
