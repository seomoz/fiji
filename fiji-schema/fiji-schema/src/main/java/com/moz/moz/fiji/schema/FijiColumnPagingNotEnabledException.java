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
 * Thrown when a client attempts to fetch the next page of data from a Fiji column, but paging is
 * not enabled on the column.
 *
 * <p>To enable paging on a column, use the
 * {@link com.moz.fiji.schema.FijiDataRequestBuilder.ColumnsDef#withPageSize(int)}
 * method in your {@link com.moz.fiji.schema.FijiDataRequestBuilder}.</p>
 */
@ApiAudience.Public
@ApiStability.Stable
public final class FijiColumnPagingNotEnabledException extends IOException {
  /**
   * Creates a new <code>FijiColumnPagingNotEnabledException</code> with the specified
   * detail message.
   *
   * @param message An error message.
   */
  public FijiColumnPagingNotEnabledException(String message) {
    super(message);
  }
}
