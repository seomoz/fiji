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

package com.moz.fiji.schema.cassandra;

import java.nio.ByteBuffer;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.commons.ByteUtils;

/**
 * A Cassandra column name.
 */
@ApiAudience.Public
@ApiStability.Evolving
public final class CassandraColumnName {

  /** The Cassandra column family. */
  private final byte[] mFamily;

  /** The Cassandra column qualifier. */
  private final byte[] mQualifier;

  /**
   * Creates a new {@link CassandraColumnName} instance.
   *
   * @param family Cassandra column family, non null.
   * @param qualifier Cassandra column qualifier, nullable (must be null if family is null).
   */
  public CassandraColumnName(byte[] family, byte[] qualifier) {
    Preconditions.checkNotNull(family, "Family must not be null.");
    mFamily = family;
    mQualifier = qualifier;
  }

  /**
   * Creates a new {@link CassandraColumnName} instance.
   *
   * @param family Cassandra column family, non-null.
   * @param qualifier Cassandra column qualifier, nullable (must be null if family is null).
   */
  public CassandraColumnName(ByteBuffer family, ByteBuffer qualifier) {
    Preconditions.checkNotNull(family, "Family must not be null.");
    mFamily = ByteUtils.toBytes(family);
    mQualifier = qualifier == null ? null : ByteUtils.toBytes(qualifier);
  }

  /**
   * Gets the Cassandra column family.  Do *not* mutate the returned byte array.
   *
   * @return the family.
   */
  public byte[] getFamily() {
    return mFamily;
  }

  /**
   * Gets the Cassandra column qualifier.  Do *not* mutate the returned byte array.
   *
   * @return The qualifier.
   */
  public byte[] getQualifier() {
    return mQualifier;
  }

  /**
   * Gets the Cassandra column family.  Do *not* mutate the bytes in the returned byte buffer.
   *
   * @return the family.
   */
  public ByteBuffer getFamilyBuffer() {
    return mFamily == null ? null : ByteBuffer.wrap(mFamily);
  }

  /**
   * Gets the Cassandra column qualifier.  Do *not* mutate the bytes in the returned byte buffer.
   *
   * @return The qualifier.
   */
  public ByteBuffer getQualifierBuffer() {
    return mQualifier == null ? null : ByteBuffer.wrap(mQualifier);
  }

  /**
   * Returns true if this {@link CassandraColumnName} includes a qualifier.
   *
   * @return if this {@link CassandraColumnName} includes a qualifier.
   */
  public boolean containsQualifier() {
    return mQualifier != null;
  }



  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects
        .toStringHelper(CassandraColumnName.class)
        .add("family", Bytes.toStringBinary(mFamily))
        .add("qualifier", Bytes.toStringBinary(mQualifier))
        .toString();
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(mFamily)
        .append(mQualifier)
        .toHashCode();
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof CassandraColumnName)) {
      return false;
    }
    final CassandraColumnName other = (CassandraColumnName) obj;
    return new EqualsBuilder()
        .append(mFamily, other.mFamily)
        .append(mQualifier, other.mQualifier)
        .isEquals();
  }
}
