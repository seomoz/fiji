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

package com.moz.fiji.schema.impl;

import java.io.IOException;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.filter.FijiColumnFilter;

/**
 * A FijiColumnFilter that allows for pagination over the qualifiers in a row, along with
 * other filters that are and-ed together.
 *
 * <p>Note that starting in HBase 0.94, this will only return at most one value, regardless
 * of where maxVersions is set to (because the HBase Filter return code is set to
 * INCLUDE_AND_NEXT_COL semantics). Because of this, we have enforced a lack of options
 * regarding maxVersions for this filter.</p>
 *
 * <p>This should only be used internally e.g. in HBaseQualifierPager to retrieve qualifiers
 * or other single-valued output.</p>
 */
@ApiAudience.Private
public final class FijiPaginationFilter extends FijiColumnFilter {
  private static final long serialVersionUID = 1L;

  /** The max number of qualifiers to return. */
  private final int mMaxQualifiers;

  /** How many versions back in history to start looking. */
  private final int mOffset = 0;

  /** Other filters to be checked before the pagination filter. */
  private final FijiColumnFilter mInputFilter;

  /**
   * Initialize pagination filter with default settings.
   *
   * @param maxQualifiers Maximum number of qualifiers to return. Must be >= 1.
   */
  public FijiPaginationFilter(int maxQualifiers) {
    Preconditions.checkArgument(maxQualifiers >= 1,
        "Invalid maximum number of qualifiers to return: %s", maxQualifiers);
    mInputFilter = null;
    mMaxQualifiers = maxQualifiers;
  }

  /**
   * Initialize pagination filter with other filters to fold in.
   *
   * @param filter Other filter that will precede
   * @param maxQualifiers Maximum number of qualifiers to return. Must be >= 1.
   */
  public FijiPaginationFilter(FijiColumnFilter filter, int maxQualifiers) {
    Preconditions.checkArgument(maxQualifiers >= 1,
        "Invalid maximum number of qualifiers to return: %s", maxQualifiers);
    mInputFilter = filter;
    mMaxQualifiers = maxQualifiers;
  }

  /** {@inheritDoc} */
  @Override
  public Filter toHBaseFilter(FijiColumnName fijiColumnName, Context context) throws IOException {
    FilterList requestFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    // Order that filters get added matters. Earlier in the list, the higher priority.
    if (mInputFilter != null) {
      requestFilter.addFilter(mInputFilter.toHBaseFilter(fijiColumnName, context));
    }
    Filter paginationFilter = new ColumnPaginationFilter(mMaxQualifiers, mOffset);
    requestFilter.addFilter(paginationFilter);
    return requestFilter;
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof FijiPaginationFilter)) {
      return false;
    } else {
      final FijiPaginationFilter otherFilter = (FijiPaginationFilter) other;
      return Objects.equal(otherFilter.mMaxQualifiers, mMaxQualifiers)
          && Objects.equal(otherFilter.mOffset, mOffset)
          && Objects.equal(otherFilter.mInputFilter, mInputFilter);
    }
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hashCode(mMaxQualifiers, mOffset, mInputFilter);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(FijiPaginationFilter.class)
        .add("max-qualifiers", mMaxQualifiers)
        .add("offset", mOffset)
        .add("filter", mInputFilter)
        .toString();
  }
}
