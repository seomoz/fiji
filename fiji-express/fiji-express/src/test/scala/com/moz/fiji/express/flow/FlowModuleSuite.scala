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

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import com.moz.fiji.express.flow.framework.FijiScheme
import com.moz.fiji.schema.FijiInvalidNameException

@RunWith(classOf[JUnitRunner])
class FlowModuleSuite extends FunSuite {
  val tableURI = "fiji://.env/default/table"

  test("Flow module forbids creating an input map-type column with a qualifier in the column "
      + "name.") {
    intercept[FijiInvalidNameException] {
      ColumnFamilyInputSpec("info:word")
    }
  }

  test("Flow module forbids creating an output map-type column with a qualifier in the column "
      + "name.") {
    intercept[IllegalArgumentException] {
      ColumnFamilyOutputSpec.builder
          .withFamily("info:word")
          .withQualifierSelector('foo)
          .build
    }
  }

  test("Flow module permits creating an output map-type column specifying the qualifier field") {
    ColumnFamilyOutputSpec.builder
        .withFamily("searches")
        .withQualifierSelector('terms)
        .build
  }

  test("Flow module permits specifying a qualifier regex on ColumnFamilyInputSpec.") {
    val colReq = ColumnFamilyInputSpec.builder
        .withFamily("search")
        .withFilterSpec(ColumnFilterSpec.Regex(""".*\.com"""))
        .build

    // TODO: Test it filters keyvalues correctly.
    assert(colReq.filterSpec.isInstanceOf[ColumnFilterSpec.Regex])
  }

  test("Flow module permits specifying a qualifier regex (with filter) on ColumnFamilyInputSpec.") {
    val colReq = ColumnFamilyInputSpec.builder
        .withFamily("search")
        .withFilterSpec(ColumnFilterSpec.Regex(""".*\.com"""))
        .build

    // TODO: Test it filters keyvalues correctly.
    assert(colReq.filterSpec.isInstanceOf[ColumnFilterSpec.Regex])
  }

  test("Flow module permits specifying versions on map-type columns without qualifier regex.") {
    val colReq = ColumnFamilyInputSpec("search", maxVersions=2)
    assert(2 === colReq.maxVersions)
  }

  test("Flow module permits specifying versions on a group-type column.") {
    val colReq = QualifiedColumnInputSpec.builder
        .withColumn("info", "word")
        .withMaxVersions(2)
        .build
    assert(2 === colReq.maxVersions)
  }

  test("Flow module uses default versions of 1 for all ColumnInputSpecs.") {
    val groupReq = QualifiedColumnInputSpec("info", "word")
    val mapReq = ColumnFamilyInputSpec("searches")

    assert(1 === groupReq.maxVersions)
    assert(1 === mapReq.maxVersions)
  }

  test("Flow module permits creating inputs and outputs with no mappings.") {
    val input: FijiSource = FijiInput.builder
        .withTableURI(tableURI)
        .withColumns(Map[String, Symbol]())
        .build
    val output: FijiSource = FijiOutput.builder
        .withTableURI(tableURI)
        .withColumnSpecs(Map[Symbol, ColumnOutputSpec]())
        .build

    assert(input.inputColumns.isEmpty)
    assert(input.outputColumns.isEmpty)
    assert(output.inputColumns.isEmpty)
    assert(output.outputColumns.isEmpty)
  }

  test("Flow module permits creating FijiSources as inputs with default options.") {
    val input: FijiSource = FijiInput.builder
        .withTableURI(tableURI)
        .withColumns("info:word" -> 'word)
        .build
    val expectedScheme = new FijiScheme(
        tableAddress = tableURI,
        timeRange = TimeRangeSpec.All,
        timestampField = None,
        icolumns = Map("word" -> QualifiedColumnInputSpec("info", "word")),
        rowRangeSpec = RowRangeSpec.All,
        rowFilterSpec = RowFilterSpec.NoFilter)

    assert(expectedScheme === input.fijiScheme)
  }

  test("Flow module permits specifying timerange for FijiInput.") {
    val input = FijiInput.builder
        .withTableURI(tableURI)
        .withTimeRangeSpec(TimeRangeSpec.Between(0L,40L))
        .withColumns("info:word" -> 'word)
        .build
    val expectedScheme = new FijiScheme(
        tableURI,
        TimeRangeSpec.Between(0L, 40L),
        None,
        Map("word" -> QualifiedColumnInputSpec("info", "word")),
        rowRangeSpec = RowRangeSpec.All,
        rowFilterSpec = RowFilterSpec.NoFilter)

    assert(expectedScheme === input.fijiScheme)
  }

  test("Flow module permits creating FijiSources with multiple columns.") {
    val input: FijiSource = FijiInput.builder
        .withTableURI(tableURI)
        .withColumns("info:word" -> 'word, "info:title" -> 'title)
        .build
    val expectedScheme: FijiScheme = {
      new FijiScheme(
          tableURI,
          TimeRangeSpec.All,
          None,
          Map(
              "word" -> QualifiedColumnInputSpec("info", "word"),
              "title" -> QualifiedColumnInputSpec("info", "title")),
        rowRangeSpec = RowRangeSpec.All,
        rowFilterSpec = RowFilterSpec.NoFilter)
    }

    assert(expectedScheme === input.fijiScheme)
  }

  test("Flow module permits specifying options for a column.") {
    FijiInput.builder
        .withTableURI(tableURI)
        .withColumns("info:word" -> 'word)
        .build

    FijiInput.builder
        .withTableURI(tableURI)
        .withColumnSpecs(QualifiedColumnInputSpec.builder
            .withColumn("info", "word")
            .withMaxVersions(1)
            .build -> 'word)
        .build

    FijiInput.builder
        .withTableURI(tableURI)
        .withColumnSpecs(ColumnFamilyInputSpec.builder
            .withFamily("searches")
            .withMaxVersions(1)
            .withFilterSpec(ColumnFilterSpec.Regex(".*"))
            .build -> 'word)
        .build
  }

  test("Flow module permits specifying different options for different columns.") {
    FijiInput.builder
        .withTableURI(tableURI)
        .withColumnSpecs(QualifiedColumnInputSpec.builder
                .withColumn("info", "word")
                .withMaxVersions(1)
                .build -> 'word,
            QualifiedColumnInputSpec.builder
                .withColumn("info", "title")
                .withMaxVersions(2)
                .build -> 'title)
        .build
  }

  test("Flow module permits creating FijiSource with the default timestamp field") {
    val output: FijiSource = FijiOutput.builder
        .withTableURI(tableURI)
        .withColumns('words -> "info:words")
        .build
    val expectedScheme: FijiScheme = new FijiScheme(
        tableAddress = tableURI,
        timeRange = TimeRangeSpec.All,
        timestampField = None,
        ocolumns = Map("words" -> QualifiedColumnOutputSpec.builder
            .withColumn("info", "words")
            .build),
        rowRangeSpec = RowRangeSpec.All,
        rowFilterSpec = RowFilterSpec.NoFilter)
    assert(expectedScheme === output.fijiScheme)
  }

  test("Flow module permits creating FijiSource with a timestamp field") {
    val output: FijiSource = FijiOutput.builder
        .withTableURI(tableURI)
        .withTimestampField('time)
        .withColumns('words -> "info:words")
        .build
    val expectedScheme: FijiScheme = new FijiScheme(
        tableAddress = tableURI,
        timeRange = TimeRangeSpec.All,
        timestampField = Some('time),
        ocolumns = Map("words" -> QualifiedColumnOutputSpec.builder
            .withColumn("info", "words")
            .build),
        rowRangeSpec = RowRangeSpec.All,
        rowFilterSpec = RowFilterSpec.NoFilter)
    assert(expectedScheme === output.fijiScheme)
  }
}
