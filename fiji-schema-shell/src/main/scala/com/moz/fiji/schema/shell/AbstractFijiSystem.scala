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

package com.moz.fiji.schema.shell

import java.io.IOException

import scala.collection.JavaConversions._
import scala.collection.mutable.Map

import org.apache.avro.Schema
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.annotations.Inheritance
import com.moz.fiji.schema.Fiji
import com.moz.fiji.schema.FijiMetaTable
import com.moz.fiji.schema.FijiURI
import com.moz.fiji.schema.avro.AvroSchema
import com.moz.fiji.schema.avro.TableLayoutDesc
import com.moz.fiji.schema.layout.FijiTableLayout
import com.moz.fiji.schema.security.FijiSecurityManager
import com.moz.fiji.schema.util.ProtocolVersion

/**
 * Abstract base interface implemented by FijiSystem. Provides method signatures
 * that access and modify underlying resources in FijiSchema.
 */
@ApiAudience.Framework
@ApiStability.Evolving
@Inheritance.Sealed
abstract class AbstractFijiSystem {
  /**
   * Gets a collection of Fiji table names and descriptions for the specified Fiji instance.
   *
   * @param uri The Fiji instance whose tables should be listed.
   * @return An array of pairs, where each pair contains the name of a Fiji table and the
   *     description for that Fiji table.
   */
  def getTableNamesDescriptions(uri: FijiURI): Array[(String, String)]

  /**
   * Gets the layout for the specified Fiji table.
   *
   * @param uri of the Fiji instance containing the table.
   * @param table whose layout should be retrieved.
   * @return The table layout, or None if the layout cannot be retrieved.
   */
  def getTableLayout(uri: FijiURI, table: String): Option[FijiTableLayout]

  /** Create a new table in the specified Fiji instance. */
  def createTable(uri: FijiURI, layout: FijiTableLayout, numRegions: Int): Unit

  /** Apply a table layout update to a Fiji table in the specified instance. */
  def applyLayout(uri: FijiURI, table: String, layout: TableLayoutDesc): Unit

  /** Drop a table. */
  def dropTable(uri: FijiURI, table: String): Unit

  /**
   * Return the instance data format (system version) of the specified Fiji instance.
   *
   * @param uri is the FijiURI of the instance to open.
   * @return the ProtocolVersion of the corresponding data format (e.g. "system-2.0")
   */
  def getSystemVersion(uri: FijiURI): ProtocolVersion

  /**
   * Return a schema id associated with a given schema, creating an association if necessary.
   *
   * @param uri of the Fiji instance we are operating in.
   * @param schema to lookup.
   * @return its existing uid, or a new id if this schema has not been encountered before.
   */
  def getOrCreateSchemaId(uri: FijiURI, schema: Schema): Long

  /**
   * Return a schema id associated with a given schema, or None if it's not already there.
   *
   * @param uri of the Fiji instance we are operating in.
   * @param schema to lookup.
   * @return its existing uid, or None if this schema has not been encountered before.
   */
  def getSchemaId(uri: FijiURI, schema: Schema): Option[Long]

  /**
   * Return a schema associated with a given schema id, or None if it's not already there.
   *
   * @param uri of the Fiji instance we are operating in.
   * @param uid of the schema id to lookup.
   * @return its existing schema, or None if this schema id has not been encountered before.
   */
  def getSchemaForId(uri: FijiURI, uid: Long): Option[Schema]

  /**
   * Return a schema associated with a given AvroSchema descriptor.
   *
   * @param uri is the Fiji instance we are operating in
   * @param schema is the Avro schema descriptor to evaluate
   * @return its existing schema, or None if this schema id has not been encountered before.
   */
  def getSchemaFor(uri: FijiURI, schema: AvroSchema): Option[Schema]

  /**
   * Set a metatable (key, val) pair for the specified table.
   *
   * @param uri of the Fiji instance.
   * @param table is the name of the table owning the property.
   * @param key to set.
   * @param value to set it to.
   * @throws IOException if there's an error opening the metatable.
   */
  def setMeta(uri: FijiURI, table: String, key: String, value: String): Unit

  /**
   * Get a metatable (key, val) pair for the specified table.
   *
   * @param uri of the Fiji instance.
   * @param table is the name of the table owning the property.
   * @param key specifying the property to retrieve.
   * @return the value of the property or None if it was not yet set.
   * @throws IOException if there's an error opening the metatable.
   */
  def getMeta(uri: FijiURI, table: String, key: String): Option[String]

  /**
   * Get the security manager for the specified instance.
   *
   * @param instanceURI is the FijiURI of the instance to get a security manager for.
   * @return the security manager for the instance.
   */
  def getSecurityManager(instanceURI: FijiURI): FijiSecurityManager

  /**
   * @return a list of all available Fiji instances.
   */
  def listInstances(): Set[String]

  /**
   * Close all Fiji-related resources opened by this object.
   */
  def shutdown(): Unit
}
