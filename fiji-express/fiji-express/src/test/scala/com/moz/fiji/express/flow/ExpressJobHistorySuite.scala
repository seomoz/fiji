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

package com.moz.fiji.express.flow

import java.io.InputStream

import scala.collection.JavaConverters.mapAsScalaMapConverter

import com.twitter.scalding.Args
import com.twitter.scalding.GroupBuilder
import com.twitter.scalding.Local
import com.twitter.scalding.Mode
import com.twitter.scalding.NullSource
import com.twitter.scalding.Stat
import org.apache.commons.io.IOUtils
import org.junit.Assert
import org.junit.Test

import com.moz.fiji.schema.Fiji
import com.moz.fiji.schema.FijiClientTest
import com.moz.fiji.schema.FijiTable
import com.moz.fiji.schema.FijiURI
import com.moz.fiji.schema.shell.api.Client
import com.moz.fiji.schema.util.InstanceBuilder
import com.moz.fiji.express.flow.framework.ExpressJobHistoryFijiTable
import com.moz.fiji.express.avro.generated.ExpressJobHistoryEntry

class ExpressJobHistorySuite extends FijiClientTest {
  import ExpressJobHistorySuite._

  @Test
  def testSimpleFlow(): Unit = {
    val fiji: Fiji = getFiji
    createTableFromDDL(DdlPath, fiji.getURI)
    val table: FijiTable = fiji.openTable(TableName)
    try {
      new InstanceBuilder(fiji)
          .withTable(table)
              .withRow("row1")
                  .withFamily("info")
                      .withQualifier("name").withValue("name1")
                      .withQualifier("email").withValue("email1")
              .withRow("row2")
                  .withFamily("info")
                      .withQualifier("name").withValue("name2")
                      .withQualifier("email").withValue("email2")
          .build()

      val extendedInfo: Map[String, String] = Map(
        "testkey" -> "testvalue",
        "testkey2" -> "testvalue2"
      )
      val args = Mode.putMode(
        Local(strictSources = false),
        Args(
          List("--tableUri", table.getURI.toString, "--extendedInfo") ++ extendedInfo.map {
            kv: (String, String) => val (k, v) = kv; "%s:%s".format(k, v)
          }
        )
      )

      val job: SimpleJob = new SimpleJob(args)
      Assert.assertTrue(job.flowCounters.isEmpty)
      Assert.assertTrue(job.run)
      Assert.assertFalse(job.flowCounters.isEmpty)

      val expressJobHistoryTable: ExpressJobHistoryFijiTable =  ExpressJobHistoryFijiTable(fiji)
      try {
        val jobDetails: ExpressJobHistoryEntry = expressJobHistoryTable
          .getExpressJobDetails(job.uniqueId.get)
        Assert.assertEquals(job.uniqueId.get, jobDetails.getJobId)
        Assert.assertEquals(job.name, jobDetails.getJobName)
        val testCounters: Set[(String, String, Long)] = job.flowCounters.filter {
          triple: (String, String, Long) => {
            val (group, name, _) = triple
            group == "group" && name == "name"
          }
        }
        Assert.assertEquals(1, testCounters.size)
        Assert.assertEquals(5, testCounters.head._3)
        val recordedExtendedInfo: Map[String, String] = jobDetails.getExtendedInfo.asScala.map {
          // We know that the elements of this pair are Strings, but Avro and Scala can't seem to
          // agree, so we just toString() them.
          pair: (Any, Any) => val (k, v) = pair; (k.toString, v.toString)
        }.toMap
        Assert.assertEquals(extendedInfo, recordedExtendedInfo)
      } finally {
        expressJobHistoryTable.close()
      }
    } finally {
      table.release()
    }
  }
}

object ExpressJobHistorySuite {
  private final val DdlPath: String = "layout/com.moz.fiji.express.flow.ITSimpleFlow.ddl"
  private final val TableName: String = "table"

  /**
   * Applies a table's DDL definition on the specified Fiji instance.
   *
   * @param resourcePath Path of the resource containing the DDL to create the table.
   * @param instanceURI URI of the Fiji instance to use.
   * @throws IOException on I/O error.
   */
  def createTableFromDDL(resourcePath: String, instanceURI: FijiURI): Unit = {
    val client: Client = Client.newInstance(instanceURI)
    try {
      val ddl: String = readResource(resourcePath)
      client.executeUpdate(ddl)
    } finally {
      client.close()
    }
  }

  /**
   * Loads a text resource by name.
   *
   * @param resourcePath Path of the resource to load.
   * @return the resource content, as a string.
   * @throws IOException on I/O error.
   */
  def readResource(resourcePath: String): String = {
    val istream: InputStream = getClass.getClassLoader.getResourceAsStream(resourcePath)
    try {
      val content: String = IOUtils.toString(istream)
      return content
    } finally {
      istream.close()
    }
  }

  class SimpleJob(args: Args) extends FijiJob(args) {
    val tableUri: String = args("tableUri")
    val stat: Stat = Stat("name", "group")

    FijiInput.builder
        .withTableURI(tableUri)
        .withColumns("info:email" -> 'email)
        .build
        .map('email -> 'email) { email: String => stat.inc; email}
        .groupAll {
          group: GroupBuilder => group.foldLeft('email -> 'size)(0) {
            (acc: Int, next: String) => {
              stat.inc; acc + 1
            }
          }
        }
        .map('size -> 'size) { email: String => stat.inc; email}
        .debug
        .write(NullSource)
  }
}
