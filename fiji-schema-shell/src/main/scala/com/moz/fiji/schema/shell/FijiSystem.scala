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
import com.moz.fiji.schema.Fiji
import com.moz.fiji.schema.FijiMetaTable
import com.moz.fiji.schema.FijiSchemaTable
import com.moz.fiji.schema.FijiURI
import com.moz.fiji.schema.avro.AvroSchema
import com.moz.fiji.schema.avro.TableLayoutDesc
import com.moz.fiji.schema.layout.FijiTableLayout
import com.moz.fiji.schema.security.FijiSecurityManager
import com.moz.fiji.schema.util.ProtocolVersion
import com.moz.fiji.schema.util.ResourceUtils
import com.moz.fiji.schema.util.VersionInfo

/**
 * Instances of this class provide the Fiji schema shell with access to FijiSchema.
 * Clients of this class should use it to obtain handles to Fiji resources.  Clients
 * should avoid closing any Fiji resources they obtain through this object.  Instead,
 * clients should use the {@link FijiSystem#shutdown} method to shutdown all
 * resources when done with interacting with Fiji.
 *
 * <p>Each thread should create its own FijiSystem instance.</p>
 */
@ApiAudience.Private
final class FijiSystem extends AbstractFijiSystem {
  // A map from Fiji instance names to internal implementations of Fiji instances.
  private val fijis = Map[FijiURI, Fiji]()

  // A lazily-initialized HBaseAdmin.
  private var maybeHBaseAdmin: Option[HBaseAdmin] = None

  /** Return an HBaseAdmin if we have one, initializing one if we don't. */
  private def hBaseAdmin: HBaseAdmin = {
    maybeHBaseAdmin match {
      case Some(admin) => admin
      case None => {
        val admin = new HBaseAdmin(HBaseConfiguration.create())
        maybeHBaseAdmin = Some(admin)
        admin
      }
    }
  }

  /**
   * Gets the meta table for the Fiji instance with the specified name.
   *
   * @param uri Name of the Fiji instance.
   * @return A meta table for the specified Fiji instance, or None if the specified instance
   *     cannot be opened.
   */
  private def fijiMetaTable(uri: FijiURI): Option[FijiMetaTable] = {
    fijiCache(uri) match {
      case Some(theFiji) => Some(theFiji.getMetaTable())
      case None => None
    }
  }

  override def getSystemVersion(uri: FijiURI): ProtocolVersion = {
    return VersionInfo.getClusterDataVersion(
        fijiCache(uri).getOrElse(throw new DDLException("Could not open " + uri)))
  }

  override def getOrCreateSchemaId(uri: FijiURI, schema: Schema): Long = {
    val fiji: Fiji = fijiCache(uri).getOrElse(throw new DDLException("Could not open " + uri))
    val schemaTable: FijiSchemaTable = fiji.getSchemaTable()
    val id = schemaTable.getOrCreateSchemaId(schema)
    schemaTable.flush()
    return id
  }

  override def getSchemaId(uri: FijiURI, schema: Schema): Option[Long] = {
    val fiji: Fiji = fijiCache(uri).getOrElse(throw new DDLException("Could not open " + uri))
    val schemaTable: FijiSchemaTable = fiji.getSchemaTable()

    // Look up the schema entry. If none exists, return None. Otherwise return Some(theId).
    return Option(schemaTable.getSchemaEntry(schema)).map { _.getId}
  }

  override def getSchemaForId(uri: FijiURI, uid: Long): Option[Schema] = {
    val fiji: Fiji = fijiCache(uri).getOrElse(throw new DDLException("Could not open " + uri))
    val schemaTable: FijiSchemaTable = fiji.getSchemaTable()

    return Option(schemaTable.getSchema(uid))
  }

  override def getSchemaFor(uri: FijiURI, avroSchema: AvroSchema): Option[Schema] = {
    if (avroSchema.getJson != null) {
      val schema: Schema = new Schema.Parser().parse(avroSchema.getJson)
      return Some(schema)
    }
    return getSchemaForId(uri, avroSchema.getUid)
  }

  override def setMeta(uri: FijiURI, table: String, key: String, value: String): Unit = {
    val metaTable: FijiMetaTable = fijiMetaTable(uri).getOrElse(
        throw new IOException("Cannot get metatable for URI " + uri))
    metaTable.putValue(table, key, value.getBytes())
  }

  override def getMeta(uri: FijiURI, table: String, key: String): Option[String] = {
    val metaTable: FijiMetaTable = fijiMetaTable(uri).getOrElse(
        throw new IOException("Cannot get metatable for URI " + uri))
    try {
      val bytes: Array[Byte] = metaTable.getValue(table, key)
      return Some(new String(bytes, "UTF-8"))
    } catch {
      case ioe: IOException => return None // Key not found.
    }
  }

  override def getSecurityManager(uri: FijiURI): FijiSecurityManager = {
    fijiCache(uri) match {
      case Some(fiji) =>
        return fiji.getSecurityManager()
      case None =>
        throw new IOException("Cannot open fiji: %s".format(uri.toString))
    }
  }

  /**
   * Gets the Fiji instance implementation for the Fiji instance with the specified name.
   *
   * <p>This method caches the Fiji instances opened.</p>
   *
   * @param uri Name of the Fiji instance.
   * @return An Fiji for the Fiji instance with the specified name, or none if the
   *     instance specified cannot be opened.
   */
  private def fijiCache(uri: FijiURI): Option[Fiji] = {
    if (!fijis.contains(uri)) {
      try {
        val theFiji = Fiji.Factory.open(uri)
        fijis += (uri -> theFiji)
        Some(theFiji)
      } catch {
        case exn: Exception => {
          exn.printStackTrace()
          None
        }
      }
    } else {
      Some(fijis(uri))
    }
  }

  override def getTableNamesDescriptions(uri: FijiURI): Array[(String, String)] = {
    // Get all table names.
    val tableNames: List[String] = fijiCache(uri) match {
      case Some(fiji) => fiji.getTableNames().toList
      case None => List()
    }
    // join table names and descriptions.
    tableNames.map { name =>
      fijiMetaTable(uri) match {
        case Some(metaTable) => {
          val description = metaTable.getTableLayout(name).getDesc().getDescription()
          (name, description)
        }
        case None => (name, "")
      }
    }.toArray
  }

  override def getTableLayout(uri: FijiURI, table: String): Option[FijiTableLayout] = {
    fijiMetaTable(uri) match {
      case Some(metaTable) => {
        try {
          val layout = metaTable.getTableLayout(table)
          Some(layout)
        } catch {
          case _: IOException => None
        }
      }
      case None => None
    }
  }

  override def createTable(uri: FijiURI, layout: FijiTableLayout, numRegions: Int): Unit = {
    fijiCache(uri) match {
      case Some(fiji) => { fiji.createTable(layout.getDesc(), numRegions) }
      case None => { throw new IOException("Cannot get fiji for \"" + uri.toString() + "\"") }
    }
  }

  override def applyLayout(uri: FijiURI, table: String, layout: TableLayoutDesc): Unit = {
    fijiCache(uri) match {
      case Some(fiji) => { fiji.modifyTableLayout(layout, false, Console.out) }
      case None => { throw new IOException("Cannot get fiji for \"" + uri.toString() + "\"") }
    }
  }

  override def dropTable(uri: FijiURI, table: String): Unit = {
    fijiCache(uri) match {
      case Some(fiji) => { fiji.deleteTable(table) }
      case None => { throw new IOException("Cannot get fiji for \"" + uri.toString() + "\"") }
    }
  }

  override def listInstances(): Set[String] = {
    def parseInstanceName(fijiTableName: String): Option[String] = {
      val parts: Seq[String] = fijiTableName.split('.')
      if (parts.length < 3 || !FijiURI.FIJI_SCHEME.equals(parts.head)) {
        None
      } else {
        Some(parts(1))
      }
    }

    val hTableDescriptors: List[HTableDescriptor] = hBaseAdmin.listTables().toList;
    val fijiInstanceNames: Set[String] = hTableDescriptors.foldLeft(Set():Set[String])({
      (names: Set[String], htableDesc) =>
        val instanceName: Option[String] = parseInstanceName(htableDesc.getNameAsString())
        instanceName match {
          case Some(instance) => { names + instance }
          case None => { names }
        }
    })
    return fijiInstanceNames
  }

  override def shutdown(): Unit = {
    maybeHBaseAdmin match {
      case None => { /* do nothing. */ }
      case Some(admin) => {
        // Close this.
        ResourceUtils.closeOrLog(hBaseAdmin)
        maybeHBaseAdmin = None
      }
    }

    fijis.foreach { case (key, refCountable) =>
        ResourceUtils.releaseOrLog(refCountable) }
    fijis.clear
  }
}
