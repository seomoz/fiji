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

package com.moz.fiji.schema.hbase;

import java.util.regex.Pattern;

import com.google.common.base.Joiner;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.schema.NotAFijiManagedTableException;

/**
 * <p>Multiple instances of Fiji can be installed on a single HBase
 * cluster.  Within a Fiji instance, several HBase tables are created
 * to manage system, metadata, schemas, and user-space tables.  This
 * class represents the name of one of those HBase tables that are
 * created and managed by Fiji.  This class should only be used internally
 * in Fiji modules, or by framework application developers who need
 * direct access to HBase tables managed by Fiji.</p>
 *
 * <p>
 * The names of tables in HBase created and managed by Fiji are
 * made of a list of delimited components.  There are at least 3
 * components of a name:
 *
 * <ol>
 *   <li>
 *     Prefix: a literal string "fiji" used to mark that this table
 *     is managed by Fiji.
 *   </li>
 *   <li>
 *     FijiInstance: the name of fiji instance managing this table.
 *   </li>
 *   <li>
 *     Type: the type of table (system, schema, meta, table).
 *   </li>
 * </ol>
 *
 * If the type of the table is "table", then it's name (the name users
 * of Fiji would use to refer to it) is the fourth and final component.
 * </p>
 *
 * <p>
 * For example, an HBase cluster might have the following tables:
 * <pre>
 * devices
 * fiji.default.meta
 * fiji.default.schema
 * fiji.default.schema_hash
 * fiji.default.schema_id
 * fiji.default.system
 * fiji.default.table.foo
 * fiji.default.table.bar
 * fiji.experimental.meta
 * fiji.experimental.schema
 * fiji.experimental.schema_hash
 * fiji.experimental.schema_id
 * fiji.experimental.system
 * fiji.experimental.table.baz
 * </pre>
 *
 * In this example, there is an HBase table completely unrelated to
 * fiji called "devices."  There are two fiji installations, one
 * called "default" and another called "experimental."  Within the
 * "default" installation, there are two Fiji tables, "foo" and
 * "bar."  Within the "experimental" installation, there is a single
 * Fiji table "baz."
 * </p>
 */
@ApiAudience.Framework
@ApiStability.Evolving
public final class FijiManagedHBaseTableName {
  /** The delimited used to separate the components of an HBase table name. */
  private static final char DELIMITER = '.';

  private static final Joiner DELIMITER_JOINER = Joiner.on(DELIMITER);

  /** The first component of all HBase table names managed by Fiji. */
  public static final String KIJI_COMPONENT = "fiji";

  /** Regexp matching Fiji system tables. */
  public static final Pattern KIJI_SYSTEM_TABLES_REGEX =
      Pattern.compile("fiji[.](.*)[.](meta|system|schema_hash|schema_id)");

  /** The name component used for the Fiji meta table. */
  private static final String KIJI_META_COMPONENT = "meta";

  /** The name component used for the Fiji schema hash table. */
  private static final String KIJI_SCHEMA_HASH_COMPONENT = "schema_hash";

  /** The name component used for the Fiji schema IDs table. */
  private static final String KIJI_SCHEMA_ID_COMPONENT = "schema_id";

  /** The name component used for the Fiji system table. */
  private static final String KIJI_SYSTEM_COMPONENT = "system";

  /** The name component used for all user-space Fiji tables. */
  private static final String KIJI_TABLE_COMPONENT = "table";

  /** The HBase table name. */
  private final String mHBaseTableName;

  /** The Fiji instance name. */
  private final String mFijiInstanceName;

  /** The Fiji table name, or null if it is not a user-space Fiji table. */
  private final String mFijiTableName;

  /**
   * Constructs a Fiji-managed HBase table name.
   *
   * @param fijiInstanceName The fiji instance name.
   * @param type The type component of the HBase table name (meta, schema, system, table).
   */
  private FijiManagedHBaseTableName(String fijiInstanceName, String type) {
    mHBaseTableName = DELIMITER_JOINER.join(KIJI_COMPONENT, fijiInstanceName, type);
    mFijiInstanceName = fijiInstanceName;
    mFijiTableName = null;
  }

  /**
   * Constructs a Fiji-managed HBase table name.
   *
   * @param fijiInstanceName The fiji instance name.
   * @param type The type component of the HBase table name.
   * @param fijiTableName The name of the user-space Fiji table.
   */
  private FijiManagedHBaseTableName(String fijiInstanceName, String type, String fijiTableName) {
    mHBaseTableName = DELIMITER_JOINER.join(KIJI_COMPONENT, fijiInstanceName, type, fijiTableName);
    mFijiInstanceName = fijiInstanceName;
    mFijiTableName = fijiTableName;
  }

  /**
   * Constructs using an HBase HTable name.
   *
   * @param hbaseTableName The HBase HTable name.
   * @return A new fiji-managed HBase table name.
   * @throws NotAFijiManagedTableException If the HBase table is not managed by fiji.
   */
  public static FijiManagedHBaseTableName get(String hbaseTableName)
      throws NotAFijiManagedTableException {
    // Split it into components.
    String[] components = StringUtils.splitPreserveAllTokens(
        hbaseTableName, Character.toString(DELIMITER), 4);

    // Make sure the first component is 'fiji'.
    if (!components[0].equals(KIJI_COMPONENT)) {
      throw new NotAFijiManagedTableException(
          hbaseTableName, "Doesn't start with fiji name component.");
    }

    if (components.length == 3) {
      // It's a managed fiji meta/schema/system table.
      return new FijiManagedHBaseTableName(components[1], components[2]);
    } else if (components.length == 4) {
      // It's a user-space fiji table.
      return new FijiManagedHBaseTableName(components[1], components[2], components[3]);
    } else {
      // Wrong number of components... must not be a fiji table.
      throw new NotAFijiManagedTableException(
          hbaseTableName, "Invalid number of name components.");
    }
  }

  /**
   * Gets a new instance of a Fiji-managed HBase table that holds the Fiji meta table.
   *
   * @param fijiInstanceName The name of the Fiji instance.
   * @return The name of the HBase table used to store the Fiji meta table.
   */
  public static FijiManagedHBaseTableName getMetaTableName(String fijiInstanceName) {
    return new FijiManagedHBaseTableName(fijiInstanceName, KIJI_META_COMPONENT);
  }

  /**
   * Gets a new instance of a Fiji-managed HBase table that holds the Fiji schema hash table.
   *
   * @param fijiInstanceName The name of the Fiji instance.
   * @return The name of the HBase table used to store the Fiji schema hash table.
   */
  public static FijiManagedHBaseTableName getSchemaHashTableName(String fijiInstanceName) {
    return new FijiManagedHBaseTableName(fijiInstanceName, KIJI_SCHEMA_HASH_COMPONENT);
  }

  /**
   * Gets a new instance of a Fiji-managed HBase table that holds the Fiji schema IDs table.
   *
   * @param fijiInstanceName The name of the Fiji instance.
   * @return The name of the HBase table used to store the Fiji schema IDs table.
   */
  public static FijiManagedHBaseTableName getSchemaIdTableName(String fijiInstanceName) {
    return new FijiManagedHBaseTableName(fijiInstanceName, KIJI_SCHEMA_ID_COMPONENT);
  }

  /**
   * Gets a new instance of a Fiji-managed HBase table that holds the Fiji system table.
   *
   * @param fijiInstanceName The name of the Fiji instance.
   * @return The name of the HBase table used to store the Fiji system table.
   */
  public static FijiManagedHBaseTableName getSystemTableName(String fijiInstanceName) {
    return new FijiManagedHBaseTableName(fijiInstanceName, KIJI_SYSTEM_COMPONENT);
  }

  /**
   * Gets a new instance of a Fiji-managed HBase table that holds a user-space Fiji table.
   *
   * @param fijiInstanceName The name of the Fiji instance.
   * @param fijiTableName The name of the user-space Fiji table.
   * @return The name of the HBase table used to store the user-space Fiji table.
   */
  public static FijiManagedHBaseTableName getFijiTableName(
      String fijiInstanceName, String fijiTableName) {
    return new FijiManagedHBaseTableName(fijiInstanceName, KIJI_TABLE_COMPONENT, fijiTableName);
  }

  /**
   * Gets the name of the Fiji instance this named table belongs to.
   *
   * @return The name of the fiji instance.
   */
  public String getFijiInstanceName() {
    return mFijiInstanceName;
  }

  /**
   * Gets the name of the Fiji table.
   * A user defined fiji table named "foo" in the default fiji instance will be stored in HBase
   * with the FijiManaged name "fiji.default.table.foo".  This method will return only "foo".
   *
   * @return The name of the fiji table, or null if this is not a user-space Fiji table.
   */
  public String getFijiTableName() {
    return mFijiTableName;
  }

  /**
   * Gets the name of the HBase table that stores the data for this Fiji table.
   *
   * @return The HBase table name as a UTF-8 encoded byte array.
   */
  public byte[] toBytes() {
    return Bytes.toBytes(mHBaseTableName);
  }

  @Override
  public String toString() {
    return mHBaseTableName;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof FijiManagedHBaseTableName)) {
      return false;
    }
    return toString().equals(other.toString());
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }
}
