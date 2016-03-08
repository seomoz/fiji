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
 * Thrown when Fiji encounters an HBase table that is not managed by Fiji.
 */
@ApiAudience.Public
@ApiStability.Stable
public final class NotAFijiManagedTableException extends Exception {
  /** The name of the HBase table. */
  private final String mHBaseTableName;

  /**
   * Creates a new <code>NotAFijiManagedTableException</code> for the specified
   * table with the specified detail message.
   *
   * @param hbaseTableName The name of an HBase table that is not managed by Fiji.
   * @param message Detail about the exception.
   */
  public NotAFijiManagedTableException(String hbaseTableName, String message) {
    super(message);
    mHBaseTableName = hbaseTableName;
  }

  /**
   * Gets the name of the HBase table that is not managed by Fiji.
   *
   * @return The HBase table name.
   */
  public String getHBaseTableName() {
    return mHBaseTableName;
  }
}
