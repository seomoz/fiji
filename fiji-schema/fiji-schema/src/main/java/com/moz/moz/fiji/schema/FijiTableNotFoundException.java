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

import java.io.IOException;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;

/**
 * Thrown when an attempt to access a table fails because it does not exist.
 */
@ApiAudience.Public
@ApiStability.Stable
public final class FijiTableNotFoundException extends IOException {
  /** URI of the missing table. */
  private final FijiURI mTableURI;

  /**
   * Creates a new <code>FijiTableNotFoundException</code> for the specified table.
   *
   * @param tableURI URI of the table that wasn't found.
   * @deprecated Use {@link FijiTableNotFoundException#FijiTableNotFoundException(FijiURI).
   */
  @Deprecated
  public FijiTableNotFoundException(String tableURI) {
    super("FijiTable not found: " + tableURI);
    mTableURI = FijiURI.newBuilder(tableURI).build();
  }

  /**
   * Creates a new <code>FijiTableNotFoundException</code> for the specified table.
   *
   * @param tableURI URI of the table that wasn't found.
   */
  public FijiTableNotFoundException(FijiURI tableURI) {
    super("FijiTable not found: " + tableURI);
    mTableURI = tableURI;
  }

  /**
   * Returns the name of the missing table.
   * @return the name of the missing table.
   */
  public String getTableName() {
    return mTableURI.getTable();
  }

  /**
   * Returns the URI of the missing table.
   * @return the URI of the missing table.
   */
  public FijiURI getTableURI() {
    return mTableURI;
  }
}
