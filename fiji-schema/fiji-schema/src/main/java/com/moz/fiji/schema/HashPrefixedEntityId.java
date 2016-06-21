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

import java.util.Arrays;
import java.util.List;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.avro.RowKeyEncoding;
import com.moz.fiji.schema.avro.RowKeyFormat;
import com.moz.fiji.schema.util.ByteArrayFormatter;
import com.moz.fiji.schema.util.Hasher;

/** Implements the hash-prefixed row key format. */
@ApiAudience.Private
public final class HashPrefixedEntityId extends EntityId {
  /** Fiji row key bytes. May be null if we only know the HBase row key. */
  private final byte[] mFijiRowKey;

  /** HBase row key bytes. */
  private final byte[] mHBaseRowKey;

  /**
   * Creates a HashPrefixedEntityId from the specified Fiji row key.
   *
   * @param fijiRowKey Fiji row key.
   * @param format Row key hashing specification.
   * @return a new HashPrefixedEntityId with the specified Fiji row key.
   */
  static HashPrefixedEntityId getEntityId(byte[] fijiRowKey, RowKeyFormat format) {
    Preconditions.checkNotNull(format);
    Preconditions.checkArgument(format.getEncoding() == RowKeyEncoding.HASH_PREFIX);
    final byte[] hash = hashFijiRowKey(format, fijiRowKey);
    final int hashSize = format.getHashSize();
    // Prepend a subset of the hash to the Fiji row key:
    final byte[] hbaseRowKey = new byte[hashSize + fijiRowKey.length];
    System.arraycopy(hash, 0, hbaseRowKey, 0, hashSize);
    System.arraycopy(fijiRowKey, 0, hbaseRowKey, hashSize, fijiRowKey.length);
    return new HashPrefixedEntityId(fijiRowKey, hbaseRowKey, format);
  }

  /**
   * Creates a HashPrefixedEntityId from the specified HBase row key.
   *
   * @param hbaseRowKey HBase row key.
   * @param format Row key hashing specification.
   * @return a new HashedEntityId with the specified HBase row key.
   */
  static HashPrefixedEntityId fromHBaseRowKey(byte[] hbaseRowKey, RowKeyFormat format) {
    Preconditions.checkNotNull(format);
    Preconditions.checkArgument(format.getEncoding() == RowKeyEncoding.HASH_PREFIX);
    final int hashSize = format.getHashSize();
    final byte[] fijiRowKey =
        Arrays.copyOfRange(hbaseRowKey, hashSize, hbaseRowKey.length);
    // TODO Paranoid expensive check : rehash the fiji key and compare with the hash?
    return new HashPrefixedEntityId(fijiRowKey, hbaseRowKey, format);
  }

  /**
   * Hashes a Fiji row key.
   *
   * @param format Hashing specification.
   * @param fijiRowKey Fiji row key.
   * @return a hash of the Fiji row key.
   */
  public static byte[] hashFijiRowKey(RowKeyFormat format, byte[] fijiRowKey) {
    // TODO refactor into hash factories:
    switch (format.getHashType()) {
    case MD5: return Hasher.hash(fijiRowKey);
    default:
      throw new RuntimeException(String.format("Unexpected hashing type: '%s'.", format));
    }
  }

  /**
   * Creates a new HashPrefixedEntityId.
   *
   * @param fijiRowKey Fiji row key.
   * @param hbaseRowKey HBase row key.
   * @param format Row key hashing specification.
   */
  private HashPrefixedEntityId(byte[] fijiRowKey, byte[] hbaseRowKey, RowKeyFormat format) {
    Preconditions.checkNotNull(format);
    Preconditions.checkArgument(format.getEncoding() == RowKeyEncoding.HASH_PREFIX);
    mFijiRowKey = Preconditions.checkNotNull(fijiRowKey);
    mHBaseRowKey = Preconditions.checkNotNull(hbaseRowKey);
  }

  /** {@inheritDoc} */
  @Override
  public byte[] getHBaseRowKey() {
    return mHBaseRowKey;
  }

  /** {@inheritDoc} **/
  @Override
  @SuppressWarnings("unchecked")
  public <T> T getComponentByIndex(int idx) {
    Preconditions.checkArgument(idx == 0);
    return (T) mFijiRowKey.clone();
  }

  /** {@inheritDoc} **/
  @Override
  public List<Object> getComponents() {
    return Lists.<Object>newArrayList(mFijiRowKey);
  }

  /** @return the Fiji row key, or null. */
  public byte[] getFijiRowKey() {
    return (mFijiRowKey != null) ? mFijiRowKey.clone() : null;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(HashPrefixedEntityId.class)
        .add("fiji", Bytes.toStringBinary(mFijiRowKey))
        .add("hbase", Bytes.toStringBinary(mHBaseRowKey))
        .toString();
  }

  /** {@inheritDoc} */
  @Override
  public String toShellString() {
    if (mFijiRowKey != null) {
      return String.format("fiji=%s", Bytes.toStringBinary(mFijiRowKey));
    } else {
    return String.format("hbase=hex:%s", ByteArrayFormatter.toHex(mHBaseRowKey));
    }
  }
}
