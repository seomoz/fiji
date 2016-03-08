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

import com.twitter.scalding.Args
import com.twitter.scalding.Hdfs
import com.twitter.scalding.Mode
import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.moz.fiji.express.FijiSuite
import com.moz.fiji.express.avro.SimpleRecord
import com.moz.fiji.express.flow.SchemaSpec.Generic
import com.moz.fiji.express.flow.SchemaSpec.Specific
import com.moz.fiji.express.flow.SchemaSpec.Writer
import com.moz.fiji.express.flow.util.ResourceUtil
import com.moz.fiji.express.flow.util.TestingResourceUtil
import com.moz.fiji.schema.{ EntityId => JEntityId }
import com.moz.fiji.schema.Fiji
import com.moz.fiji.schema.FijiClientTest
import com.moz.fiji.schema.FijiDataRequest
import com.moz.fiji.schema.FijiTable
import com.moz.fiji.schema.FijiTableReader
import com.moz.fiji.schema.FijiTableWriter
import com.moz.fiji.schema.layout.FijiTableLayout
import com.moz.fiji.express.flow.util.TestingResourceUtil

@RunWith(classOf[JUnitRunner])
class ReaderSchemaSuite extends FijiClientTest with FijiSuite {
  import com.moz.fiji.express.flow.util.AvroTypesComplete._
  setupFijiTest()
  val fiji: Fiji = createTestFiji()
  val layout: FijiTableLayout = TestingResourceUtil.layout("layout/avro-types-complete.json")
  val table: FijiTable = {
    fiji.createTable(layout.getDesc)
    fiji.openTable(layout.getName)
  }
  val conf: Configuration = getConf
  val uri: String = table.getURI.toString
  val reader: FijiTableReader = table.openTableReader()
  val writer: FijiTableWriter = table.openTableWriter()

  private def entityId(s: String): JEntityId = { table.getEntityId(s) }

  private def writeValue(eid: String, column: String, value: Any) {
    writer.put(entityId(eid), family, column, value)
    writer.flush()
  }

  private def getValue[T](eid: String, column: String): T = {
    val get = reader.get(entityId(eid), FijiDataRequest.create(family, column))
    require(get.containsColumn(family, column)) // Require the cell exists for null case
    get.getMostRecentValue(family, column)
  }

  private def testExpressReadWrite[T](
      column: String,
      value: Any,
      schemaSpec: SchemaSpec,
      overrideSchema: Option[SchemaSpec] = None
  ) {
    val readEid = column + "-in"
    val writeEid = column + "-out"
    writeValue(readEid, column, value)

    val outputSchema = overrideSchema.getOrElse(schemaSpec)

    val inputCol = QualifiedColumnInputSpec(family, column, schemaSpec = schemaSpec)
    val outputCol = QualifiedColumnOutputSpec(family, column, outputSchema)

    val argsWithMode = Mode.putMode(Hdfs(strict = true, conf), Args(Nil))

    new ReadWriteJob[T](uri, inputCol, outputCol, writeEid, argsWithMode).run
    assert(value === getValue[T](writeEid, column))
  }

  test("A FijiJob can read a counter column with the writer schema.") {
    testExpressReadWrite[Long](counterColumn, longs.head, Writer)
  }

  test("A FijiJob can read a raw bytes column with the writer schema.") {
    testExpressReadWrite[Array[Byte]](rawColumn, bytes.head, Writer)
  }

  test("A FijiJob can read a null column with the writer schema.") {
    testExpressReadWrite[Null](nullColumn, null, Writer)
  }

  test("A FijiJob can read a null column with a generic reader schema.") {
    testExpressReadWrite[Null](nullColumn, null, Generic(nullSchema))
  }

  test("A FijiJob can read a boolean column with the writer schema.") {
    testExpressReadWrite[Boolean](booleanColumn, booleans.head, Writer)
  }

  test("A FijiJob can read a boolean column with a generic reader schema.") {
    testExpressReadWrite[Boolean](booleanColumn, booleans.head, Generic(booleanSchema))
  }

  test("A FijiJob can read an int column with the writer schema.") {
    testExpressReadWrite[Int](intColumn, ints.head, Writer)
  }

  test("A FijiJob can read an int column with a generic reader schema.") {
    testExpressReadWrite[Int](intColumn, ints.head, Generic(intSchema))
  }

  test("A FijiJob can read a long column with the writer schema.") {
    testExpressReadWrite[Long](longColumn, longs.head, Writer)
  }

  test("A FijiJob can read a long column with a generic reader schema.") {
    testExpressReadWrite[Long](longColumn, longs.head, Generic(longSchema))
  }

  test("A FijiJob can read a float column with the writer schema.") {
    testExpressReadWrite[Float](floatColumn, floats.head, Writer)
  }

  test("A FijiJob can read a float column with a generic reader schema.") {
    testExpressReadWrite[Float](floatColumn, floats.head, Generic(floatSchema))
  }

  test("A FijiJob can read a double column with the writer schema.") {
    testExpressReadWrite[Double](doubleColumn, doubles.head, Writer)
  }

  test("A FijiJob can read a double column with a generic reader schema.") {
    testExpressReadWrite[Double](doubleColumn, doubles.head, Generic(doubleSchema))
  }

  /** TODO: reenable when Schema-594 is fixed. */
  ignore("A FijiJob can read a bytes column with the writer schema.") {
    testExpressReadWrite[Array[Byte]](bytesColumn, bytes.head, Writer)
  }

  /** TODO: reenable when Schema-594 is fixed. */
  ignore("A FijiJob can read a bytes column with a generic reader schema.") {
    testExpressReadWrite[Array[Byte]](bytesColumn, bytes.head, Generic(bytesSchema))
  }

  test("A FijiJob can read a string column with the writer schema.") {
    testExpressReadWrite[String](stringColumn, strings.head, Writer)
  }

  test("A FijiJob can read a string column with a generic reader schema.") {
    testExpressReadWrite[String](stringColumn, strings.head, Generic(stringSchema))
  }

  test("A FijiJob can read a specific record column with the writer schema.") {
    testExpressReadWrite[SimpleRecord](specificColumn, specificRecords.head, Writer)
  }

  test("A FijiJob can read a specific record column with a generic reader schema.") {
    testExpressReadWrite[SimpleRecord](specificColumn, specificRecords.head,
        Generic(specificSchema))
  }

  test("A FijiJob can read a specific record column with a specific reader schema.") {
    testExpressReadWrite[SimpleRecord](specificColumn, specificRecords.head,
      Specific(classOf[SimpleRecord]))
  }

  test("A FijiJob can read a generic record column with the writer schema.") {
    testExpressReadWrite[GenericRecord](genericColumn, genericRecords.head, Writer)
  }

  test("A FijiJob can read a generic record column with a generic reader schema.") {
    testExpressReadWrite[GenericRecord](genericColumn, genericRecords.head, Generic(genericSchema))
  }

  test("A FijiJob can read an enum column with the writer schema.") {
    testExpressReadWrite[GenericEnumSymbol](enumColumn, enums.head, Writer,
        Some(Generic(enumSchema)))
  }

  test("A FijiJob can read an enum column with a generic reader schema.") {
    testExpressReadWrite[String](enumColumn, enums.head, Generic(enumSchema))
  }

  test("A FijiJob can read an array column with the writer schema.") {
    testExpressReadWrite[List[String]](arrayColumn, avroArrays.head, Writer,
        Some(Generic(arraySchema)))
  }

  test("A FijiJob can read an array column with a generic reader schema.") {
    testExpressReadWrite[List[String]](arrayColumn, avroArrays.head, Generic(arraySchema))
  }

  test("A FijiJob can read a union column with the writer schema [INT].") {
    testExpressReadWrite[Any](unionColumn, ints.head, Writer, Some(Generic(unionSchema)))
  }

  test("A FijiJob can read a union column with the writer schema [STRING].") {
    testExpressReadWrite[Any](unionColumn, strings.head, Writer, Some(Generic(unionSchema)))
  }

  test("A FijiJob can read a fixed column with the writer schema [INT].") {
    testExpressReadWrite[GenericFixed](fixedColumn, fixeds.head, Writer,
        Some(Generic(fixedSchema)))
  }

  test("A FijiJob can read a fixed column with a generic reader schema.") {
    testExpressReadWrite[GenericFixed](fixedColumn, fixeds.head, Generic(fixedSchema))
  }
}

// Must be its own top-level class for mystical serialization reasons
class ReadWriteJob[T](
    uri: String,
    input: ColumnInputSpec,
    output: ColumnOutputSpec,
    writeEid: String,
    args: Args
) extends FijiJob(args) {

  /**
   * Unwraps the latest value from an iterable of cells and verifies that the type is as expected.
   *
   * @param slice containing the value to unwrap.
   * @return unwrapped value of type T.
   */
  private def unwrap(slice: Seq[FlowCell[T]]): (T, Long) = {
    require(slice.size == 1)
    val cell = slice.head
    (cell.datum, cell.version)
  }

  FijiInput.builder
      .withTableURI(uri)
      .withColumnSpecs(input -> 'slice)
      .build
      .read
      .mapTo('slice -> ('value, 'time))(unwrap)
      .map('value -> 'entityId) { _: T => EntityId(writeEid)}
      .write(FijiOutput.builder
          .withTableURI(uri)
          .withTimestampField('time)
          .withColumnSpecs(Map('value -> output))
          .build)
}

