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

import java.io.Closeable;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.annotations.Inheritance;
import com.moz.fiji.schema.avro.SystemTableBackup;
import com.moz.fiji.schema.util.CloseableIterable;
import com.moz.fiji.schema.util.ProtocolVersion;

/**
 * The Fiji system table, which stores system information such as the version, ready state, and
 * locks.
 *
 * @see FijiMetaTable
 * @see FijiSchemaTable
 */
@ApiAudience.Framework
@ApiStability.Stable
@Inheritance.Sealed
public interface FijiSystemTable extends Closeable {
  /**
   * Gets the version of fiji installed.  This refers to the version of the meta tables and other
   * administrative fiji info installed, not the client code.
   *
   * @return the version string.
   * @throws IOException If there is an error.
   */
  ProtocolVersion getDataVersion() throws IOException;

  /**
   * Sets the version of fiji installed.  This refers to the version of the meta tables and other
   * administrative fiji info installed, not the client code.
   *
   * @param version the version string.
   * @throws IOException If there is an error.
   */
  void setDataVersion(ProtocolVersion version) throws IOException;

  /**
   * Gets the version of Fiji security in this instance.  The version of Fiji Security installed
   * in this instance restricts the granularity and type of access that may be granted to users.
   *
   * @return the version string.  Version security-0.0 means security is not installed.
   * @throws IOException If there is an I/O error.
   */
  ProtocolVersion getSecurityVersion() throws IOException;

  /**
   * Sets the version of Fiji security in this instance.  The version of Fiji Security installed
   * in this instance restricts the granularity and type of access that may be granted to users.
   *
   * Only superusers and users with GRANT permission should be able to set the security version.
   * Directly setting the security version may result in inconsistent state in this instance,
   * unless an security version upgrade is performed.
   *
   * @param version the version string.
   * @throws IOException If there is an I/O error.
   */
  void setSecurityVersion(ProtocolVersion version) throws IOException;

  /**
   * Gets the value associated with a property key.
   *
   * @param key The property key to look up.
   * @return The value in the system table with the given key, or null if the key is not found.
   * @throws IOException If there is an error.
   */
  byte[] getValue(String key) throws IOException;

  /**
   * Sets a value for a property key, which creates it if it doesn't exist.
   *
   * @param key The property key to set.
   * @param value The value of the property.
   * @throws IOException If there is an error.
   */
  void putValue(String key, byte[] value) throws IOException;

  /**
   * Gets an iterator across all key value pairs in the table.
   *
   * @return an iterator of key, value pairs.
   * @throws IOException If there is an error.
   */
  CloseableIterable<SimpleEntry<String, byte[]>> getAll()
      throws IOException;

  /**
   * Returns key/value backup information in a form that can be directly written to a MetadataBackup
   * record. To read more about the avro try that has been specified to store this info, see
   * Layout.avdl
   *
   * @throws IOException in case of an error.
   * @return A SystemTableBackup record containing a list of key/value pairs.
   */
  SystemTableBackup toBackup() throws IOException;

  /**
   * Restores the system table entries from the specified backup record.
   *
   * @param backup The system table entries from a MetadataBackup record.  Each consists of a
   *    key/value pair and a timestamp.
   * @throws IOException in case of an error.
   */
  void fromBackup(SystemTableBackup backup) throws IOException;

  /**
   * @return The URI of the Fiji instance that this FijiSystemTable serves.
   */
  FijiURI getFijiURI();
}
