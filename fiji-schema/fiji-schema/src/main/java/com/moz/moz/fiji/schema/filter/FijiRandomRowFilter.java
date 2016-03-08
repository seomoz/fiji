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

package com.moz.fiji.schema.filter;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.schema.FijiDataRequest;

/**
 * A row filter that accepts or rejects rows at random.
 *
 * <p> Wraps the HBase {@link org.apache.hadoop.hbase.filter.RandomRowFilter}. </p>
 * <p> A row is included if {@code random.nextFloat() < chance}. </p>
 */
@ApiAudience.Public
@ApiStability.Experimental
public final class FijiRandomRowFilter extends FijiRowFilter {

  /** The name of the chance node. */
  private static final String CHANCE_NODE = "chance";

  /** Cutoff for random row selection: 0 means all excluded, 1 means all excluded. */
  private final float mChance;

  /**
   * Construct a filter that will return a row if {@code random.nextFloat() < chance}.
   *
   * @param chance Cutoff for random comparison selection.
   *     Chance must be between 0 an 1, inclusive: [0,1].
   *     0 means all excluded.
   *     1 means all included.
   */
  public FijiRandomRowFilter(float chance) {
    Preconditions.checkArgument(chance >= 0 && chance <= 1, "chance must be between 0 and 1");
    this.mChance = chance;
  }

  /**
   * Get the row selection chance.
   *
   * @return the cutoff for random comparison selection.
   */
  public float getChance() {
    return this.mChance;
  }

  /** {@inheritDoc} */
  @Override
  public FijiDataRequest getDataRequest() {
    return FijiDataRequest.builder().build();
  }

  /** {@inheritDoc} */
  @Override
  public Filter toHBaseFilter(Context context) throws IOException {
    return new RandomRowFilter(this.mChance);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof FijiRandomRowFilter)) {
      return false;
    } else {
      final FijiRandomRowFilter otherFijiRandomRowFilter = (FijiRandomRowFilter) other;
      return new EqualsBuilder()
          .append(this.mChance, otherFijiRandomRowFilter.mChance)
          .isEquals();
    }
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(this.mChance)
        .toHashCode();
  }

  /** {@inheritDoc} */
  @Override
  protected JsonNode toJsonNode() {
    ObjectNode rootNode = JsonNodeFactory.instance.objectNode();
    rootNode.put(CHANCE_NODE, this.mChance);
    return rootNode;
  }

  /** {@inheritDoc} */
  @Override
  protected Class<? extends FijiRowFilterDeserializer> getDeserializerClass() {
    return FijiRandomRowFilterDeserializer.class;
  }

  /**
   * A class to deserialize the FijiRandomRowFilter from a JsonNode.
   */
  public static class FijiRandomRowFilterDeserializer implements FijiRowFilterDeserializer {
    /** {@inheritDoc} */
    @Override
    public FijiRowFilter createFromJson(JsonNode rootNode) {
      float chance = (float) rootNode.path(CHANCE_NODE).getValueAsDouble();
      return new FijiRandomRowFilter(chance);
    }
  }
}
