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

package com.moz.fiji.express.flow.framework

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapred.JobConf
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import com.moz.fiji.express.FijiSuite
import com.moz.fiji.express.flow.ColumnInputSpec
import com.moz.fiji.express.flow.InvalidFijiTapException
import com.moz.fiji.express.flow.RowFilterSpec
import com.moz.fiji.express.flow.RowRangeSpec
import com.moz.fiji.express.flow.TimeRangeSpec
import com.moz.fiji.express.flow.util.ResourceUtil
import com.moz.fiji.express.flow.util.TestingResourceUtil
import com.moz.fiji.schema.FijiURI
import com.moz.fiji.schema.layout.FijiTableLayout

@RunWith(classOf[JUnitRunner])
class FijiTapSuite extends FijiSuite {
  val instanceName: String = "test_FijiTap_instance"
  val testFijiTableLayout: FijiTableLayout = TestingResourceUtil.layout("layout/avro-types.json")
  val config: JobConf = new JobConf(HBaseConfiguration.create())

  test("FijiTap validates a valid instance/table/column.") {
    val testTable = makeTestFijiTable(testFijiTableLayout, instanceName)
    val fijiURI = testTable.getURI

    val testScheme: FijiScheme = new FijiScheme(
        tableAddress = fijiURI.toString,
        timeRange = TimeRangeSpec.All,
        timestampField = None,
        icolumns = Map(
            "dummy_field1" -> ColumnInputSpec("searches"),
            "dummy_field2" -> ColumnInputSpec("family:column1")),
        rowRangeSpec = RowRangeSpec.All,
        rowFilterSpec = RowFilterSpec.NoFilter)

    val testTap: FijiTap = new FijiTap(fijiURI, testScheme)

    testTap.validate(config)
  }

  test("FijiTap validates a nonexistent instance.") {
    val testTable = makeTestFijiTable(testFijiTableLayout, instanceName)
    val fijiURI = testTable.getURI

    val testScheme: FijiScheme = new FijiScheme(
        tableAddress = fijiURI.toString,
        timeRange = TimeRangeSpec.All,
        timestampField = None,
        icolumns = Map(
            "dummy_field1" -> ColumnInputSpec("searches"),
            "dummy_field2" -> ColumnInputSpec("family:column1")),
        rowRangeSpec = RowRangeSpec.All,
        rowFilterSpec = RowFilterSpec.NoFilter)

    val testURI: FijiURI = FijiURI.newBuilder(fijiURI)
        .withInstanceName("nonexistent_instance")
        .build()

    val testTap: FijiTap = new FijiTap(testURI, testScheme)

    intercept[InvalidFijiTapException] {
      testTap.validate(config)
    }
  }

  test("FijiTap validates a nonexistent table.") {
    val testTable = makeTestFijiTable(testFijiTableLayout, instanceName)
    val fijiURI = testTable.getURI

    val testScheme: FijiScheme = new FijiScheme(
        tableAddress = fijiURI.toString,
        timeRange = TimeRangeSpec.All,
        timestampField = None,
        icolumns = Map(
            "dummy_field1" -> ColumnInputSpec("searches"),
            "dummy_field2" -> ColumnInputSpec("family:column1")),
        rowRangeSpec = RowRangeSpec.All,
        rowFilterSpec = RowFilterSpec.NoFilter)

    val testURI: FijiURI = FijiURI.newBuilder(fijiURI)
        .withTableName("nonexistent_table")
        .build()

    val testTap: FijiTap = new FijiTap(testURI, testScheme)

    intercept[InvalidFijiTapException] {
      testTap.validate(config)
    }
  }

  test("FijiTap validates a nonexistent column.") {
    val testTable = makeTestFijiTable(testFijiTableLayout, instanceName)
    val fijiURI = testTable.getURI

    val testScheme: FijiScheme = new FijiScheme(
        tableAddress = fijiURI.toString,
        timeRange = TimeRangeSpec.All,
        timestampField = None,
        icolumns = Map(
            "dummy_field1" -> ColumnInputSpec("searches"),
            "dummy_field2" -> ColumnInputSpec("family:nonexistent")),
        rowRangeSpec = RowRangeSpec.All,
        rowFilterSpec = RowFilterSpec.NoFilter)

    val testTap: FijiTap = new FijiTap(fijiURI, testScheme)

    val exception = intercept[InvalidFijiTapException] {
      testTap.validate(config)
    }

    assert(exception.getMessage.contains("nonexistent"))
  }

  test("FijiTap validates multiple nonexistent columns.") {
    val testTable = makeTestFijiTable(testFijiTableLayout, instanceName)
    val fijiURI = testTable.getURI

    val testScheme: FijiScheme = new FijiScheme(
        tableAddress = fijiURI.toString,
        timeRange = TimeRangeSpec.All,
        timestampField = None,
        icolumns = Map(
            "dummy_field1" -> ColumnInputSpec("nonexistent1"),
            "dummy_field2" -> ColumnInputSpec("family:nonexistent2")),
        rowRangeSpec = RowRangeSpec.All,
        rowFilterSpec = RowFilterSpec.NoFilter)

    val testTap: FijiTap = new FijiTap(fijiURI, testScheme)

    val exception = intercept[InvalidFijiTapException] {
      testTap.validate(config)
    }

    assert(exception.getMessage.contains("nonexistent1"))
    assert(exception.getMessage.contains("nonexistent2"))
  }
}
