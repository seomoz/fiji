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

package com.moz.fiji.schema.impl;

import java.io.IOException;
import java.util.Iterator;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Iterators;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.FijiResult;
import com.moz.fiji.schema.FijiResultScanner;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiRowScanner;
import com.moz.fiji.schema.layout.FijiTableLayout;

/**
 * A {@code FijiRowScanner} implementation backed by a {@code FijiResultScanner}.
 */
@ApiAudience.Private
public class FijiResultRowScanner implements FijiRowScanner {
  private final Iterator<FijiRowData> mResultRowScanner;
  private final FijiResultScanner<Object> mResultScanner;
  private final FijiTableLayout mLayout;

  /**
   * Constructor for {@code FijiResultRowScanner}.
   *
   * @param layout The {@code FijiTableLayout} for the table.
   * @param resultScanner The {@code FijiResultScanner} backing this {@code FijiRowScanner}. This
   * {@code FijiResultScanner} is owned by the newly created {@code FijiResultRowScanner} and
   * calling close on this {@code FijiResultRowScanner} will close the underlying
   * {@code FijiResultScanner}.
   */
  public FijiResultRowScanner(
      final FijiTableLayout layout,
      final FijiResultScanner<Object> resultScanner) {
    mLayout = layout;
    mResultScanner = resultScanner;
    mResultRowScanner = Iterators.transform(
        resultScanner,
        new Function<FijiResult<Object>, FijiRowData>() {
          @Override
          public FijiRowData apply(final FijiResult<Object> result) {
            return new FijiResultRowData(mLayout, result);
          }
        });
  }

  /** {@inheritDoc} */
  @Override
  public Iterator<FijiRowData> iterator() {
    return mResultRowScanner;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    mResultScanner.close();
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hashCode(mLayout, mResultRowScanner);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final FijiResultRowScanner other = (FijiResultRowScanner) obj;
    return Objects.equal(this.mLayout, other.mLayout)
        && Objects.equal(this.mResultRowScanner, other.mResultRowScanner);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("mLayout", mLayout)
        .add("mResultRowScanner", mResultRowScanner)
        .toString();
  }
}
