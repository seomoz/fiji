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

import scala.collection.mutable.Map

import java.util.NoSuchElementException

import org.apache.avro.Schema
import org.mockito.Mock
import org.scalatest.mock.EasyMockSugar

import com.moz.fiji.schema.Fiji
import com.moz.fiji.schema.KConstants
import com.moz.fiji.schema.FijiMetaTable
import com.moz.fiji.schema.FijiURI
import com.moz.fiji.schema.layout.FijiTableLayout
import com.moz.fiji.schema.avro.AvroSchema
import com.moz.fiji.schema.avro.TableLayoutDesc
import com.moz.fiji.schema.util.ProtocolVersion
import com.moz.fiji.schema.util.VersionInfo
import com.moz.fiji.schema.security.FijiSecurityManager

/**
 * A FijiSystem class that provides in-memory mappings from instance -&gt; table &gt; layout,
 * and does not communicate with HBase.
 */
class MockFijiSystem extends AbstractFijiSystem with EasyMockSugar {
  /** Mappings from FijiURI -> table name -> FijiTableLayout */
  private val instanceData: Map[FijiURI, Map[String, FijiTableLayout]] = Map()

  /** Mappings from FijiURI -> table name -> key -> value for the metatable. */
  private val metaData: Map[FijiURI, Map[String, Map[String, String]]] = Map()

  // A "schema table" holding schema <--> id bimappings.
  private val idsForSchemas: Map[Schema, Long] = Map()
  private val schemasForIds: Map[Long, Schema] = Map()
  private var nextSchemaId: Int = 0

  // A mock security manager.
  private val mockSecurityManager = mock[FijiSecurityManager]

  {
    val defaultURI = FijiURI.newBuilder().withInstanceName(KConstants.DEFAULT_INSTANCE_NAME).build()
    val fooURI = FijiURI.newBuilder().withInstanceName("foo").build()
    val barURI = FijiURI.newBuilder().withInstanceName("bar").build()

    // Create 3 empty instances.
    instanceData(defaultURI) = Map[String, FijiTableLayout]()
    instanceData(fooURI) = Map[String, FijiTableLayout]()
    instanceData(barURI) = Map[String, FijiTableLayout]()

    metaData(defaultURI) = Map[String, Map[String, String]]()
    metaData(fooURI) = Map[String, Map[String, String]]()
    metaData(barURI) = Map[String, Map[String, String]]()
  }

  override def getOrCreateSchemaId(uri: FijiURI, schema: Schema): Long = {
    if (!idsForSchemas.contains(schema)) {
      val id = nextSchemaId
      nextSchemaId += 1
      idsForSchemas(schema) = id
      schemasForIds(id) = schema
    }

    return idsForSchemas(schema)
  }

  override def getSchemaId(uri: FijiURI, schema: Schema): Option[Long] = {
    return idsForSchemas.get(schema)
  }

  override def getSchemaForId(uri: FijiURI, schemaId: Long): Option[Schema] = {
    return schemasForIds.get(schemaId)
  }

  override def getSchemaFor(uri: FijiURI, schema: AvroSchema): Option[Schema] = {
    if (schema.getJson != null) {
      return Some(new Schema.Parser().parse(schema.getJson))
    }
    return getSchemaForId(uri, schema.getUid)
  }

  override def getSystemVersion(uri: FijiURI): ProtocolVersion = {
    // Just return the max data version supported by this version of Fiji.
    return VersionInfo.getClientDataVersion()
  }

  override def setMeta(uri: FijiURI, table: String, key: String, value: String): Unit = {
    if (!metaData.contains(uri)) {
      metaData(uri) = Map[String, Map[String, String]]()
    }

    val instanceMap: Map[String, Map[String, String]] = metaData(uri)

    if (!instanceMap.contains(table)) {
      instanceMap(table) = Map[String, String]()
    }

    val tableMap = instanceMap(table)
    tableMap(key) = value
  }

  override def getMeta(uri: FijiURI, table: String, key: String): Option[String] = {
    try {
      Some(metaData(uri)(table)(key))
    } catch {
      case nsee: NoSuchElementException => { return None }
    }

  }

  override def getSecurityManager(uri: FijiURI): FijiSecurityManager = {
    // Return a mock security manager for testing purposes.
    return mockSecurityManager
  }

  override def getTableNamesDescriptions(uri: FijiURI): Array[(String, String)] = {
    try {
      val tables: Map[String, FijiTableLayout] = instanceData(uri)
      val out: List[(String, String)] = tables.keys.foldLeft(Nil:List[(String,String)])(
          (lst, tableName) =>
              (tableName, tables(tableName).getDesc.getDescription().toString()) :: lst
      )
      return out.toArray
    } catch {
      case nsee: NoSuchElementException => {
        throw new RuntimeException("No such instance: " + uri.toString)
      }
    }
  }

  override def getTableLayout(uri: FijiURI, table: String): Option[FijiTableLayout] = {
    try {
      Some(FijiTableLayout.newLayout(TableLayoutDesc.newBuilder(
          instanceData(uri)(table).getDesc()).build()))
    } catch {
      case nsee: NoSuchElementException => None
    }
  }

  override def createTable(uri: FijiURI, layout: FijiTableLayout, numRegions: Int): Unit = {
    // Verify that the layout has all the required values set.
    TableLayoutDesc.newBuilder(layout.getDesc()).build()
    try {
      instanceData(uri)(layout.getName())
      throw new RuntimeException("Table already exists")
    } catch {
      case nsee: NoSuchElementException => {
        try {
          val tableMap = instanceData(uri)
          tableMap(layout.getName()) = layout
        } catch {
          case nsee: NoSuchElementException => {
              throw new RuntimeException("Instance does not exist")
          }
        }
      }
    }
  }

  override def applyLayout(uri: FijiURI, table: String, layout: TableLayoutDesc): Unit = {
    try {
      // Check that the table already exists.
      instanceData(uri)(table)
      // Verify that the layout has all the required values set.
      val layoutCopy = TableLayoutDesc.newBuilder(layout).build()
      // Verify that the update is legal.
      val wrappedLayout = FijiTableLayout.createUpdatedLayout(layoutCopy, instanceData(uri)(table))
      // TODO: Process deletes manually.
      instanceData(uri)(table) = wrappedLayout // Update our copy to match.
    } catch {
      case nsee: NoSuchElementException => throw new RuntimeException(table + " doesn't exist!")
    }
  }

  override def dropTable(uri: FijiURI, table: String): Unit = {
    try {
      instanceData(uri)(table) // Verify it exists first.
      instanceData(uri).remove(table) // Then remove it.
    } catch {
      case nsee: NoSuchElementException => {
        throw new RuntimeException("missing instance or table")
      }
    }
  }

  override def listInstances(): Set[String] = {
    instanceData
        .keySet
        .foldLeft(Nil: List[String])((lst, uri) => uri.getInstance() :: lst)
        .toSet
  }

  override def shutdown(): Unit = {
  }
}
