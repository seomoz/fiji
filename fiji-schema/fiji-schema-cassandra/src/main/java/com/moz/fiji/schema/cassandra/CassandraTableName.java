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

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.layout.impl.ColumnId;

/**
 * The name of a Fiji-controlled Cassandra table name.
 *
 * <p>
 *   Multiple instances of Fiji can be installed on a single Cassandra cluster.  Within a Fiji
 *   instance, several Cassandra tables are created to manage system, metadata, schemas, and
 *   user-space tables. This class represents the name of one of those Cassandra tables that are
 *   created and managed by Fiji.  This class should only be used internally in Fiji modules, or by
 *   framework application developers who need direct access to Cassandra tables managed by Fiji.
 * </p>
 *
 * <p>
 *   The names of tables in Cassandra created and managed by Fiji are made of a list of delimited
 *   components.  There are at least 3 components of a name:
 * </p>
 *
 * <ol>
 *   <li>
 *     Prefix: a literal string "fiji" used to mark that this table is managed by Fiji.
 *   </li>
 *   <li>
 *     FijiInstance: the name of fiji instance managing this table.
 *   </li>
 *   <li>
 *     Type: the type of table (system, schema, meta, user).
 *   </li>
 *   <li>
 *     Name: if the table is a user table ("lg"), then the Fiji table's name is the fourth
 *         component.
 *   </li>
 *   <li>
 *     Name: if the table is a user table ("lg"), then the locality group ID is the fifth component.
 *   </li>
 * </ol>
 *
 * <p>
 *   For example, a Cassandra cluster might have the following tables:
 * </p>
 * <pre>
 * devices
 * fiji_default.meta
 * fiji_default.schema
 * fiji_default.schema_hash
 * fiji_default.schema_id
 * fiji_default.system
 * fiji_default.lg_foo_BB
 * fiji_default.lg_foo_BC
 * fiji_default.lg_bar_BB
 * fiji_experimental.meta
 * fiji_experimental.schema
 * fiji_experimental.schema_hash
 * fiji_experimental.schema_id
 * fiji_experimental.system
 * fiji_experimental.lg_baz_BB
 * </pre>
 *
 * <p>
 *   In this example, there is a Cassandra keyspace completely unrelated to Fiji called "devices."
 *   There are two Fiji installations, one called "default" and another called "experimental."
 *   Within the "default" installation, there are two Fiji tables, "foo" and "bar."  Within the
 *   "experimental" installation, there is a single Fiji table "baz."
 * </p>
 *
 * <p>
 *   Note that Cassandra does not allow the "." character in keyspace or table names, so the '_'
 *   character is used as a delimiter.
 * </p>
 */
@ApiAudience.Framework
@ApiStability.Evolving
public final class CassandraTableName {

  /** The first component of all Cassandra keyspaces managed by Fiji. */
  public static final String KEYSPACE_PREFIX = "fiji";

  /** The Fiji table type. */
  private final TableType mType;

  /** The Fiji instance name. */
  private final String mInstance;

  /** The Fiji table name, or null. */
  private final String mTable;

  /** The Fiji locality group ID, or null. */
  private final ColumnId mLocalityGroup;

  /** The types of Cassandra tables used by Fiji. */
  private enum TableType {
    META_KEY_VALUE("meta_key_value"),
    META_LAYOUT("meta_layout"),
    SCHEMA_HASH("schema_hash"),
    SCHEMA_ID("schema_id"),
    SCHEMA_COUNTER("schema_counter"),
    SYSTEM("system"),
    LOCALITY_GROUP("lg");

    private final String mName;

    /**
     * Default constructor.
     *
     * @param name of table type.
     */
    TableType(final String name) {
      mName = name;
    }


    /**
     * Get the table type name prefixed to table names in Cassandra.
     *
     * @return the table type prefix name.
     */
    public String getName() {
      return mName;
    }
  }

  /**
   * Constructs a Fiji-managed Cassandra table name.  The name will have quotes in it so that it
   * can be used in CQL queries without additional processing (CQL is case-insensitive without
   * quotes).
   *
   * @param type of the Cassandra table (e.g., meta, schema, system, user).
   * @param instanceName of the table.
   */
  private CassandraTableName(TableType type, String instanceName) {
    this(type, instanceName, null, null);
  }

  /**
   * Constructs a Fiji-managed Cassandra table name.  The name will have quotes in it so that it
   * can be used in CQL queries without additional processing (CQL is case-insensitive without
   * quotes).
   *
   * @param type The {@code TableType} of the table.
   * @param instance The Fiji instance the table belongs to.
   * @param table The name of the Fiji table, or null.
   * @param localityGroup The ID of the Fiji table's locality group, or null.
   */
  private CassandraTableName(
      final TableType type,
      final String instance,
      final String table,
      final ColumnId localityGroup) {
    Preconditions.checkNotNull(type);
    Preconditions.checkNotNull(instance);
    Preconditions.checkArgument(
        (type != TableType.LOCALITY_GROUP) || table != null,
        "Table name must be defined for a Fiji locality group table.");
    Preconditions.checkArgument(
        type != TableType.LOCALITY_GROUP || localityGroup != null,
        "Locality group ID must be defined for a Fiji locality group table.");

    mType = type;
    mInstance = instance;
    mTable = table;
    mLocalityGroup = localityGroup;
  }

  /**
   * Get the Cassandra table name of a Fiji meta able.
   *
   * @param fijiURI The name of the Fiji instance.
   * @return The name of the Cassandra table used to store the Fiji meta table.
   */
  public static CassandraTableName getMetaLayoutTableName(FijiURI fijiURI) {
    return new CassandraTableName(TableType.META_LAYOUT, fijiURI.getInstance());
  }

  /**
   * Get the Cassandra table name of a Fiji meta key-value table.
   *
   * @param fijiURI The name of the Fiji instance.
   * @return The name of the Cassandra table used to store the Fiji meta key-value table.
   */
  public static CassandraTableName getMetaKeyValueTableName(FijiURI fijiURI) {
    return new CassandraTableName(TableType.META_KEY_VALUE, fijiURI.getInstance());
  }

  /**
   * Get the Cassandra table name of a Fiji schema hash table.
   *
   * @param fijiURI The name of the Fiji instance.
   * @return The name of the Cassandra table used to store the Fiji schema hash table.
   */
  public static CassandraTableName getSchemaHashTableName(FijiURI fijiURI) {
    return new CassandraTableName(TableType.SCHEMA_HASH, fijiURI.getInstance());
  }

  /**
   * Get the Cassandra table name of a Fiji schema ID table.
   *
   * @param fijiURI The name of the Fiji instance.
   * @return The name of the Cassandra table used to store the Fiji schema IDs table.
   */
  public static CassandraTableName getSchemaIdTableName(FijiURI fijiURI) {
    return new CassandraTableName(TableType.SCHEMA_ID, fijiURI.getInstance());
  }

  /**
   * Get the Cassandra table name of a Fiji schema counter table.
   *
   * @param fijiURI The name of the Fiji instance.
   * @return The name of the Cassandra table used to store the Fiji schema IDs counter table.
   */
  public static CassandraTableName getSchemaCounterTableName(FijiURI fijiURI) {
    return new CassandraTableName(TableType.SCHEMA_COUNTER, fijiURI.getInstance());
  }

  /**
   * Get the Cassandra table name of a Fiji system table.
   *
   * @param fijiURI The name of the Fiji instance.
   * @return The name of the Cassandra table used to store the Fiji system table.
   */
  public static CassandraTableName getSystemTableName(FijiURI fijiURI) {
    return new CassandraTableName(TableType.SYSTEM, fijiURI.getInstance());
  }

  /**
   * Get a Cassandra table name for a Fiji table locality group.
   *
   * @param tableURI The name of the Fiji table.
   * @param localityGroupID The ID of the locality group.
   * @return The name of the Cassandra table used to store the user-space Fiji table.
   */
  public static CassandraTableName getLocalityGroupTableName(
      final FijiURI tableURI,
      final ColumnId localityGroupID
  ) {
    return new CassandraTableName(
        TableType.LOCALITY_GROUP,
        tableURI.getInstance(),
        tableURI.getTable(),
        localityGroupID);
  }

  /**
   * Returns true if this table name is for a Fiji locality group.
   *
   * @return If this table name is for a Fiji locality group.
   */
  public boolean isLocalityGroup() {
    return mType == TableType.LOCALITY_GROUP;
  }

  /**
   * Get the Cassandra keyspace (formatted for CQL) for a Fiji instance.
   *
   * @param instanceURI The URI of the Fiji instance.
   * @return The Cassandra keyspace of the instance.
   */
  public static String getKeyspace(FijiURI instanceURI) {
    return appendCassandraKeyspace(new StringBuilder().append('"'), instanceURI.getInstance())
        .append('"')
        .toString();
  }

  /**
   * Get the name of the Cassandra keyspace for a Fiji instance.
   *
   * @param instanceURI The URI of the Fiji instance.
   * @return The Cassandra keyspace.
   */
  public static String getUnquotedKeyspace(FijiURI instanceURI) {
    return appendCassandraKeyspace(new StringBuilder(), instanceURI.getInstance()).toString();
  }

  /**
   * Add the unquoted Cassandra keyspace to the provided StringBuilder, and return it.
   *
   * @param builder to add the Cassandra keyspace to.
   * @param instance name.
   * @return the builder.
   */
  private static StringBuilder appendCassandraKeyspace(StringBuilder builder, String instance) {
    // "${KEYSPACE_PREFIX}_${instanceName}"
    return builder.append(KEYSPACE_PREFIX).append('_').append(instance);
  }

  /**
   * Get the Cassandra keyspace (formatted for CQL) of the table.
   *
   * @return the quoted keyspace of the Cassandra table.
   */
  public String getKeyspace() {
    return appendCassandraKeyspace(new StringBuilder().append('"'), mInstance)
        .append('"')
        .toString();
  }

  /**
   * Get the Cassandra keyspace for the table.
   *
   * @return the keyspace of the Cassandra table.
   */
  public String getUnquotedKeyspace() {
    return appendCassandraKeyspace(new StringBuilder(), mInstance).toString();
  }

  /**
   * Get the table name of this Cassandra table name.
   *
   * @return the table name of this Cassandra table name.
   */
  public String getUnquotedTable() {
    return appendCassandraTableName(new StringBuilder()).toString();
  }

  /**
   * Get the table name of this Cassandra table name.
   *
   * the table name is formatted with quotes to be CQL-compatible.
   *
   * @return the quoted table name of this Cassandra table name.
   */
  public String getTable() {
    return appendCassandraTableName(new StringBuilder().append('"')).append('"').toString();
  }

  /**
   * Add the unquoted Cassandra table name to the provided StringBuilder, and return it.
   *
   * @param builder to add the Cassandra table name to.
   * @return the builder.
   */
  private StringBuilder appendCassandraTableName(StringBuilder builder) {
    // "${type}[_${table_name}][_${locality_group_id}]
    return Joiner.on('_').skipNulls().appendTo(builder, mType.getName(), mTable, mLocalityGroup);
  }

  /**
   * Gets the Fiji instance of this Cassandra table.
   *
   * @return the Fiji instance.
   */
  public String getFijiInstance() {
    return mInstance;
  }

  /**
   * Gets the Fiji table of this Cassandra table.
   *
   * @return the Fiji table.
   */
  public String getFijiTable() {
    return mTable;
  }

  /**
   * Gets the locality group ID of this Cassandra table.
   *
   * @return the
   */
  public ColumnId getLocalityGroupId() {
    return mLocalityGroup;
  }

  /**
   * Get the Cassandra-formatted name for this table.
   *
   * The name include the keyspace, and is formatted with quotes so that it is ready to get into a
   * CQL query.
   *
   * @return The Cassandra-formatted name of this table.
   */
  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append('"');
    appendCassandraKeyspace(builder, mInstance);
    builder.append("\".\"");
    appendCassandraTableName(builder);
    builder.append('"');
    return builder.toString();
  }

  /** {@inheritDoc}. */
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof CassandraTableName)) {
      return false;
    }
    final CassandraTableName other = (CassandraTableName) obj;
    return Objects.equal(mType, other.mType)
        && Objects.equal(mInstance, other.mInstance)
        && Objects.equal(mTable, other.mTable);
  }

  /** {@inheritDoc}. */
  @Override
  public int hashCode() {
    return Objects.hashCode(mType, mInstance, mTable);
  }
}
