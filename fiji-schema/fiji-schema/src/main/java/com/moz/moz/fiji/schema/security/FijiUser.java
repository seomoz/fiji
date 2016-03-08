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

package com.moz.fiji.schema.security;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.hbase.security.User;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.schema.avro.FijiUserRecord;
import com.moz.fiji.schema.avro.SecurityUserList;

/**
 * FijiUser represents a user of Fiji in the context of Fiji's security model.
 *
 * A user does not need to have been authenticated for a FijiUser class to be created.
 */
@ApiAudience.Framework
@ApiStability.Experimental
public final class FijiUser implements Comparable<FijiUser> {
  /** Internal byte representation of the user's name. */
  private final String mName;

  /**
   * Constructs a new FijiUser from a name. Usernames must be non-null and non-empty.
   *
   * @param name of the user.
   * @throws IllegalArgumentException if the name is not a legal FijiUser name. Usernames must be
   *     non-null and non-empty.
   */
  private FijiUser(String name) {
    if (null == name || name.isEmpty()) {
      throw new IllegalArgumentException("Name of a FijiUser may not be null or empty.");
    }
    mName = name;
  }

  /**
   * Constructs a new FijiUser from a name.
   *
   * @param name of the user.
   * @return a new FijiUser with name 'name'.
   * @throws IllegalArgumentException if the name is not a legal FijiUser name.
   */
  public static FijiUser fromName(String name) {
    return new FijiUser(name);
  }

  /**
   * Gets the name of this FijiUser as a String.
   *
   * @return the name of this FijiUser.
   */
  public String getName() {
    return mName;
  }

  /**
   * Gets the name of this FijiUser, in bytes.
   *
   * @return the name of this FijiUser.
   */
  public byte[] getNameBytes() {
    return mName.getBytes(Charsets.UTF_8);
  }

  /**
   * Returns a string representation of this FijiUser.
   *
   * @return a string representation of this FijiUser.
   */
  @Override
  public String toString() {
    return getName();
  }

  /**
   * Gets the current FijiUser.
   *
   * @return the current FijiUser.
   * @throws IOException on I/O error.
   */
  public static FijiUser getCurrentUser() throws IOException {
    return new FijiUser(User.getCurrent().getName());
  }

  /**
   * Serializes a set of FijiUsers to a byte array.
   *
   * @param users to serialize.
   * @return serialized users.
   * @throws IOException on I/O error.
   */
  public static byte[] serializeFijiUsers(Set<FijiUser> users) throws IOException {
    // Create list of FijiUserRecords.
    List<FijiUserRecord> userRecordList = Lists.newArrayList();
    for (FijiUser user: users) {
      userRecordList.add(FijiUserRecord.newBuilder().setName(user.getName()).build());
    }
    // Create SecurityUserList to serialize.
    SecurityUserList userList = SecurityUserList.newBuilder().setUsers(userRecordList).build();

    // Write it out.
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
    SpecificDatumWriter<SecurityUserList> writer =
        new SpecificDatumWriter<SecurityUserList>(SecurityUserList.getClassSchema());
    writer.write(userList, encoder);
    encoder.flush();
    ByteBuffer serialized = ByteBuffer.allocate(out.toByteArray().length);
    serialized.put(out.toByteArray());
    return serialized.array();
  }

  /**
   * Deserializes a set of FijiUsers from a byte array.
   *
   * @param bytes to deserialize.
   * @return deserialized users.
   * @throws IOException on I/O error.
   */
  public static Set<FijiUser> deserializeFijiUsers(byte[] bytes) throws IOException {
    SpecificDatumReader<SecurityUserList> reader =
        new SpecificDatumReader<SecurityUserList>(SecurityUserList.SCHEMA$);
    Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
    SecurityUserList securityUserList = reader.read(null, decoder);

    Set<FijiUser> result = new HashSet<FijiUser>();
    for (FijiUserRecord userRecord : securityUserList.getUsers()) {
      result.add(FijiUser.fromName(userRecord.getName()));
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (null == other || other.getClass() != this.getClass()) {
      return false;
    }
    FijiUser otherUser = (FijiUser) other;
    return Objects.equal(otherUser.getName(), this.getName());
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hashCode(getName());
  }

  /** {@inheritDoc} */
  @Override
  public int compareTo(FijiUser otherUser) {
    return otherUser.getName().compareTo(this.getName());
  }
}
