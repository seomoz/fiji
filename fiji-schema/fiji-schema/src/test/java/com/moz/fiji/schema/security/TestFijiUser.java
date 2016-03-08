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

package com.moz.fiji.schema.security;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

/** Unit tests for FijiUser. */
public class TestFijiUser {
  @Test
  public void testSerializeDeserialize() throws Exception {
    FijiUser user1 = FijiUser.fromName("boop");
    FijiUser user2 = FijiUser.fromName("bip");
    Set<FijiUser> usersToSerialize = new HashSet<FijiUser>();
    usersToSerialize.add(user1);
    usersToSerialize.add(user2);
    byte[] serialized = FijiUser.serializeFijiUsers(usersToSerialize);
    Set<FijiUser> deserializedUsers = FijiUser.deserializeFijiUsers(serialized);
    assert (deserializedUsers.size() == 2);
    assert (deserializedUsers.contains(user1));
    assert (deserializedUsers.contains(user2));
  }
}
