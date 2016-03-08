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

package com.moz.fiji.express.flow

import java.io.InputStream

import org.apache.commons.io.IOUtils
import org.apache.hadoop.mapred.JobConf
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.twitter.scalding.Args
import com.twitter.scalding.Hdfs
import com.twitter.scalding.Mode
import com.twitter.scalding.NullSource
import org.junit.Assert

import com.moz.fiji.schema.Fiji
import com.moz.fiji.schema.FijiURI
import com.moz.fiji.schema.shell.api.Client
import com.moz.fiji.schema.testutil.AbstractFijiIntegrationTest
import com.moz.fiji.schema.util.InstanceBuilder

class IntegrationTestSimpleFlow extends AbstractFijiIntegrationTest {
  private final val Log: Logger = LoggerFactory.getLogger(classOf[IntegrationTestSimpleFlow])

  private final val TestLayout: String = "layout/com.moz.fiji.express.flow.ITSimpleFlow.ddl"
  private final val TableName: String = "table"

  /**
   * Applies a table's DDL definition on the specified Fiji instance.
   *
   * @param resourcePath Path of the resource containing the DDL to create the table.
   * @param instanceURI URI of the Fiji instance to use.
   * @throws IOException on I/O error.
   */
  def create(resourcePath: String, instanceURI: FijiURI): Unit = {
    val client: Client = Client.newInstance(instanceURI)
    try {
      val ddl: String = readResource(resourcePath)
      Log.info("Executing DDL statement:\n{}", ddl)
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
    Log.info("Reading resource '{}'.", resourcePath)
    val istream: InputStream = getClass.getClassLoader().getResourceAsStream(resourcePath)
    try {
      val content: String = IOUtils.toString(istream)
      Log.info("Resource content is:\n{}", content)
      return content
    } finally {
      istream.close()
    }
  }

  @Test
  def testSimpleFlow(): Unit = {
    val fijiURI = getFijiURI()
    create(TestLayout, fijiURI)

    val fiji = Fiji.Factory.open(fijiURI)
    try {
      val table = fiji.openTable(TableName)
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

        class Job(args: Args) extends FijiJob(args) {
          FijiInput.builder
              .withTableURI(table.getURI.toString)
              .withColumns("info:email" -> 'email)
              .build
              .groupAll { group => group.size }
              .debug
              .write(NullSource)
        }
        val args = Mode.putMode(Hdfs(false, conf = new JobConf(getConf)), Args(List()))
        Assert.assertTrue(new Job(args).run)
      } finally {
        table.release()
      }
    } finally {
      fiji.release()
    }
  }
}
