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

package com.moz.fiji.schema.filter;

import java.util.List;

import org.codehaus.jackson.JsonNode;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;

/**
 * Combines a list of row filters using a logical OR operator.
 *
 * <p> Column filters are applied in order and lazily. </p>
 */
@ApiAudience.Public
@ApiStability.Evolving
public final class OrRowFilter extends OperatorRowFilter {
  /**
   * Creates a row filter that combines a list of row filters with an OR operator.
   *
   * @param filters Row filters to combine with a logical OR.
   *     Nulls are filtered out.
   * @deprecated Use {@link Filters#or(FijiRowFilter...)}.
   */
  @Deprecated
  public OrRowFilter(List<? extends FijiRowFilter> filters) {
    super(OperatorRowFilter.Operator.OR, filters.toArray(new FijiRowFilter[filters.size()]));
  }

  /**
   * Creates a row filter that combines a list of row filters with an OR operator.
   *
   * @param filters Row filters to combine with a logical OR.
   *     Nulls are filtered out.
   * @deprecated Use {@link Filters#or(FijiRowFilter...)}.
   */
  @Deprecated
  public OrRowFilter(FijiRowFilter... filters) {
    super(OperatorRowFilter.Operator.OR, filters);
  }

  /** {@inheritDoc} */
  @Override
  protected Class<? extends FijiRowFilterDeserializer> getDeserializerClass() {
    return OrRowFilterDeserializer.class;
  }

  /** Deserializes {@code OrRowFilter}. */
  public static final class OrRowFilterDeserializer implements FijiRowFilterDeserializer {
    /** {@inheritDoc} */
    @Override
    public FijiRowFilter createFromJson(JsonNode root) {
      final List<FijiRowFilter> filters = OperatorRowFilter.parseFilterList(root);
      return new OrRowFilter(filters);
    }
  }
}
