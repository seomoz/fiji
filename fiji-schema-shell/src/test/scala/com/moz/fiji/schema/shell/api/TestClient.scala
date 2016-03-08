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

package com.moz.fiji.schema.shell.api

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._

import org.specs2.mutable._
import org.apache.hadoop.hbase.HBaseConfiguration
import com.moz.fiji.schema.Fiji
import com.moz.fiji.schema.FijiInstaller
import com.moz.fiji.schema.FijiURI
import com.moz.fiji.schema.avro.FamilyDesc
import com.moz.fiji.schema.layout.FijiTableLayout
import com.moz.fiji.schema.shell.DDLException
import com.moz.fiji.schema.shell.Environment
import com.moz.fiji.schema.shell.FijiSystem
import com.moz.fiji.schema.shell.input.NullInputSource

/** Tests that the api.Client interface does the right thing. */
class TestClient extends SpecificationWithJUnit {
  "The Client API" should {
    "create a table correctly" in {
      val uri = getNewInstanceURI()
      val client = Client.newInstance(uri)
      client.executeUpdate("""
          |CREATE TABLE foo WITH DESCRIPTION 'some data'
          |ROW KEY FORMAT HASHED
          |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
          |  MAXVERSIONS = INFINITY,
          |  TTL = FOREVER,
          |  INMEMORY = false,
          |  COMPRESSED WITH GZIP,
          |  FAMILY info WITH DESCRIPTION 'basic information' (
          |    name "string" WITH DESCRIPTION 'The user\'s name',
          |    email "string",
          |    age "int"),
          |  MAP TYPE FAMILY integers COUNTER
          |);""".stripMargin('|'))

      // Programmatically test proper table creation.
      // Check that we have created as many locgroups, map families, and group families
      // as we expect to be here.
      val environment = env(uri)
      val maybeLayout = environment.fijiSystem.getTableLayout(uri, "foo")
      maybeLayout must beSome[FijiTableLayout]
      val layout = maybeLayout.get.getDesc
      val locGroups = layout.getLocalityGroups()
      locGroups.size mustEqual 1
      val defaultLocGroup = locGroups.head
      defaultLocGroup.getName().toString() mustEqual "default"
      defaultLocGroup.getFamilies().size mustEqual 2

      (defaultLocGroup.getFamilies().filter({grp => grp.getName().toString() == "integers"})
          .size mustEqual 1)
      val maybeInfo = defaultLocGroup.getFamilies().find({ grp =>
          grp.getName().toString() == "info" })
      maybeInfo must beSome[FamilyDesc]
      maybeInfo.get.getColumns().size mustEqual 3

      client.close()
      environment.fijiSystem.shutdown()

      ok("Completed test")
    }

    "tolerate statements not terminated with a semicolon" in {
      val uri = getNewInstanceURI()
      val client = Client.newInstance(uri)
      client.executeUpdate("""
          |CREATE TABLE foo WITH DESCRIPTION 'some data'
          |ROW KEY FORMAT HASHED
          |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
          |  MAXVERSIONS = INFINITY,
          |  TTL = FOREVER,
          |  INMEMORY = false,
          |  COMPRESSED WITH GZIP,
          |  FAMILY info WITH DESCRIPTION 'basic information' (
          |    name "string" WITH DESCRIPTION 'The user\'s name',
          |    email "string",
          |    age "int"),
          |  MAP TYPE FAMILY integers COUNTER
          |)""".stripMargin('|'))
      client.close()

      ok("Completed test")
    }

    "handle sequential clients" in {
      val uri = getNewInstanceURI()
      val client = Client.newInstance(uri)
      client.executeUpdate("""
          |CREATE TABLE foo WITH DESCRIPTION 'some data'
          |ROW KEY FORMAT HASHED
          |WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
          |  MAXVERSIONS = INFINITY,
          |  TTL = FOREVER,
          |  INMEMORY = false,
          |  COMPRESSED WITH GZIP,
          |  FAMILY info WITH DESCRIPTION 'basic information' (
          |    name "string" WITH DESCRIPTION 'The user\'s name',
          |    email "string",
          |    age "int")
          |)""".stripMargin('|'))
      client.close()

      val client2 = Client.newInstance(uri)
      client2.executeUpdate("DROP TABLE foo")
      client.close()

      ok("Completed test")
    }

    "throw on syntax err" in {
      val uri = getNewInstanceURI()
      val client = Client.newInstance(uri)
      try {
        client.executeUpdate("THIS IS NOT A VALID CREATE TABLE STATEMENT")
        println("Got this instead: " + client.getLastOutput())
        failure("This should have failed")
      } catch {
        case e: DDLException =>
          println("Got expected exception: " + e.getMessage())
      } finally {
        client.close()
      }

      ok("Completed test")
    }

    "throw on semantic err" in {
      val uri = getNewInstanceURI()
      val client = Client.newInstance(uri)
      try {
        client.executeUpdate("DROP TABLE neverthereinthefirstplace;")
        failure("This should have failed")
      } catch {
        case e: DDLException =>
          println("Got expected exception: " + e.getMessage())
      } finally {
        client.close()
      }

      ok("Completed test")
    }
  }

  private val mNextInstanceId = new AtomicInteger(0);

  /**
   * @return the name of a unique Fiji instance (that doesn't yet exist).
   */
  def getNewInstanceURI(): FijiURI = {
    val id = mNextInstanceId.incrementAndGet()
    val uri = FijiURI.newBuilder().withZookeeperQuorum(Array(".fake." +
        id)).withInstanceName("default").build()
    installFiji(uri)
    return uri
  }

  /**
   * Install a Fiji instance.
   */
  def installFiji(instanceURI: FijiURI): Unit = {
    FijiInstaller.get().install(instanceURI, HBaseConfiguration.create())
  }

  private def env(uri: FijiURI) = {
    new Environment(
        uri,
        System.out,
        new FijiSystem,
        new NullInputSource(),
        List(),
        false)
  }
}
