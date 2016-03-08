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

import org.specs2.mutable._

import com.moz.fiji.schema.FijiURI
import com.moz.fiji.schema.avro.RowKeyEncoding
import com.moz.fiji.schema.avro.RowKeyFormat
import com.moz.fiji.schema.avro.TableLayoutDesc
import com.moz.fiji.schema.KConstants
import com.moz.fiji.schema.layout.FijiTableLayout
import com.moz.fiji.schema.util.VersionInfo
import java.util.ArrayList

import com.moz.fiji.schema.shell.ddl.CreateTableCommand
import com.moz.fiji.schema.shell.input.NullInputSource

class TestMockFijiSystem extends SpecificationWithJUnit {
  val defaultURI = FijiURI.newBuilder().withInstanceName(KConstants.DEFAULT_INSTANCE_NAME).build()

  "MockFijiSystem" should {
    "include three instances" in {
      val instances = new MockFijiSystem().listInstances()
      instances.size mustEqual 3
      instances.contains(KConstants.DEFAULT_INSTANCE_NAME) mustEqual true
      instances.contains("foo") mustEqual true
      instances.contains("a-missing-instance") mustEqual false
    }

    "allow create table" in {
      val avro: TableLayoutDesc = new TableLayoutDesc
      avro.setLocalityGroups(new ArrayList())
      avro.setVersion(CreateTableCommand.DDL_LAYOUT_VERSION.toString())

      avro.setName("t")
      avro.setDescription("desc")
      val rowKeyFormat = new RowKeyFormat
      rowKeyFormat.setEncoding(RowKeyEncoding.HASH)
      avro.setKeysFormat(rowKeyFormat)
      val layout = FijiTableLayout.newLayout(avro)
      val sys = new MockFijiSystem
      sys.createTable(defaultURI, layout, 1)
      (sys.getTableNamesDescriptions(defaultURI)
          mustEqual List(("t", "desc")).toArray)
    }

    "support the Environment.containsTable operation" in {
      val avro: TableLayoutDesc = new TableLayoutDesc
      avro.setLocalityGroups(new ArrayList())
      avro.setVersion(CreateTableCommand.DDL_LAYOUT_VERSION.toString())

      avro.setName("t")
      avro.setDescription("desc")
      val rowKeyFormat = new RowKeyFormat
      rowKeyFormat.setEncoding(RowKeyEncoding.HASH)
      avro.setKeysFormat(rowKeyFormat)
      val layout = FijiTableLayout.newLayout(avro)
      val sys = new MockFijiSystem
      sys.createTable(defaultURI, layout, 1)

      new Environment(defaultURI, Console.out,
        sys, new NullInputSource, List(), false).containsTable("t") mustEqual true
    }

    "allow drop table" in {
      val avro: TableLayoutDesc = new TableLayoutDesc
      avro.setLocalityGroups(new ArrayList())
      avro.setVersion(CreateTableCommand.DDL_LAYOUT_VERSION.toString())

      avro.setName("t")
      avro.setDescription("desc")
      val rowKeyFormat = new RowKeyFormat
      rowKeyFormat.setEncoding(RowKeyEncoding.HASH)
      avro.setKeysFormat(rowKeyFormat)
      val layout = FijiTableLayout.newLayout(avro)
      val sys = new MockFijiSystem
      sys.createTable(defaultURI, layout, 1)
      (sys.getTableNamesDescriptions(defaultURI)
          mustEqual List(("t", "desc")).toArray)
      sys.dropTable(defaultURI, "t")
      (sys.getTableNamesDescriptions(defaultURI)
          mustEqual List[(String, String)]().toArray)
    }

    "disallow create table twice on the same name" in {
      val avro: TableLayoutDesc = new TableLayoutDesc
      avro.setLocalityGroups(new ArrayList())
      avro.setVersion(CreateTableCommand.DDL_LAYOUT_VERSION.toString())
      val sys = new MockFijiSystem

      avro.setName("t")
      avro.setDescription("desc")
      val rowKeyFormat = new RowKeyFormat
      rowKeyFormat.setEncoding(RowKeyEncoding.HASH)
      avro.setKeysFormat(rowKeyFormat)
      val layout = FijiTableLayout.newLayout(avro)
      sys.createTable(defaultURI, layout, 1)
      (sys.createTable(defaultURI, layout, 1)
          must throwA[RuntimeException])
    }

    "disallow drop table on missing table" in {
      val sys = new MockFijiSystem
      sys.dropTable(defaultURI, "t") must throwA[RuntimeException]
    }

    "disallow apply layout on missing table" in {
      val sys = new MockFijiSystem
      val avro: TableLayoutDesc = new TableLayoutDesc
      avro.setLocalityGroups(new ArrayList())
      avro.setVersion(CreateTableCommand.DDL_LAYOUT_VERSION.toString())
      val rowKeyFormat = new RowKeyFormat
      rowKeyFormat.setEncoding(RowKeyEncoding.HASH)
      avro.setKeysFormat(rowKeyFormat)
      avro.setName("t")
      avro.setDescription("meep")
      // Verify that this is a valid layout
      FijiTableLayout.newLayout(avro)
      // .. but you can't apply it to a missing table.
      (sys.applyLayout(defaultURI, "t", avro)
          must throwA[RuntimeException])
    }

    "createTable should fail on malformed input records" in {
      val sys = new MockFijiSystem
      val avro: TableLayoutDesc = new TableLayoutDesc // Missing the localityGroups list, etc.
      FijiTableLayout.newLayout(avro) must throwA[RuntimeException]
    }

    "update layout with applyLayout" in {
      val avro: TableLayoutDesc = new TableLayoutDesc
      avro.setLocalityGroups(new ArrayList())
      avro.setVersion(CreateTableCommand.DDL_LAYOUT_VERSION.toString())
      val rowKeyFormat = new RowKeyFormat
      rowKeyFormat.setEncoding(RowKeyEncoding.HASH)
      avro.setKeysFormat(rowKeyFormat)
      avro.setName("t")
      avro.setDescription("desc1")
      val layout: FijiTableLayout = FijiTableLayout.newLayout(avro)
      val sys = new MockFijiSystem

      sys.createTable(defaultURI, layout, 1)
      (sys.getTableNamesDescriptions(defaultURI)
          mustEqual List(("t", "desc1")).toArray)

      val avro2: TableLayoutDesc = new TableLayoutDesc
      avro2.setLocalityGroups(new ArrayList())
      avro2.setVersion(CreateTableCommand.DDL_LAYOUT_VERSION.toString())
      avro2.setName("t")
      avro2.setDescription("desc2")
      avro2.setKeysFormat(rowKeyFormat)
      sys.applyLayout(defaultURI, "t", avro2)
      (sys.getTableNamesDescriptions(defaultURI)
          mustEqual List(("t", "desc2")).toArray)
    }

    "getTableLayout() should deep copy Avro records given to client" in {
      val avro: TableLayoutDesc = new TableLayoutDesc
      avro.setLocalityGroups(new ArrayList())
      avro.setVersion(CreateTableCommand.DDL_LAYOUT_VERSION.toString())
      avro.setName("t")
      avro.setDescription("desc1")
      val rowKeyFormat = new RowKeyFormat
      rowKeyFormat.setEncoding(RowKeyEncoding.HASH)
      avro.setKeysFormat(rowKeyFormat)
      val layout: FijiTableLayout = FijiTableLayout.newLayout(avro)
      val sys = new MockFijiSystem

      sys.createTable(defaultURI, layout, 1)
      (sys.getTableNamesDescriptions(defaultURI)
          mustEqual List(("t", "desc1")).toArray)

      val maybeLayout2 = sys.getTableLayout(defaultURI, "t")
      maybeLayout2 must beSome[FijiTableLayout]

      val layout2 = maybeLayout2 match {
        case Some(layout) => layout
        case None => throw new RuntimeException("Missing!")
      }

      layout2.getDesc().setDescription("desc2") // Prove that this updates a copy...

      // By verifying that the MockFijiSystem returns the original description.
      (sys.getTableNamesDescriptions(defaultURI)
          mustEqual List(("t", "desc1")).toArray)
    }

    "setMeta and getMeta should preserve values" in {
      val sys = new MockFijiSystem
      (sys.getMeta(defaultURI, "someTable", "myKey")) must beNone
      sys.setMeta(defaultURI, "someTable", "myKey", "myVal")

      val ret: Option[String] = sys.getMeta(defaultURI, "someTable", "myKey")
      ret must beSome[String]
      ret.get mustEqual "myVal"
    }
  }
}
