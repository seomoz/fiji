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

import scala.collection.JavaConversions

import cascading.tuple.Fields
import com.twitter.scalding.Args
import com.twitter.scalding.Hdfs
import com.twitter.scalding.IterableSource
import com.twitter.scalding.Mode
import com.twitter.scalding.TupleConverter
import com.twitter.scalding.TupleSetter
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.Fixed
import org.apache.hadoop.conf.Configuration
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import com.moz.fiji.express.FijiSuite
import com.moz.fiji.express.flow.SchemaSpec.Generic
import com.moz.fiji.express.flow.SchemaSpec.Writer
import com.moz.fiji.express.flow.util.AvroTypesComplete
import com.moz.fiji.schema.Fiji
import com.moz.fiji.schema.FijiClientTest
import com.moz.fiji.schema.FijiColumnName
import com.moz.fiji.schema.FijiDataRequest
import com.moz.fiji.schema.FijiTable
import com.moz.fiji.schema.FijiTableReader
import com.moz.fiji.schema.FijiTableWriter
import com.moz.fiji.schema.{EntityId => SchemaEntityId}

@RunWith(classOf[JUnitRunner])
class WriterSchemaSuite extends FijiClientTest with FijiSuite {
  import WriterSchemaSuite._
  import AvroTypesComplete._

  // TODO: These non-test things can be moved to the companion object after SCHEMA-539 fix
  setupFijiTest()
  val fiji: Fiji = createTestFiji()
  val table: FijiTable = {
    fiji.createTable(AvroTypesComplete.layout.getDesc)
    fiji.openTable(AvroTypesComplete.layout.getName)
  }
  val conf: Configuration = getConf
  val uri = table.getURI.toString
  val reader: FijiTableReader = table.openTableReader()
  val writer: FijiTableWriter = table.openTableWriter()

  /**
   * Get value from HBase.
   * @param eid string of row
   * @param column column containing requested value
   * @tparam T expected type of value
   * @return the value
   */
  def getValue[T](eid: String, column: FijiColumnName): T = {
    def entityId(s: String): SchemaEntityId = table.getEntityId(s)
    val (family, qualifier) = column.getFamily -> column.getQualifier
    val get = reader.get(entityId(eid), FijiDataRequest.create(family, qualifier))
    require(get.containsColumn(family, qualifier)) // Require the cell exists for null case
    get.getMostRecentValue(family, qualifier)
  }

  /**
   * Verify that the inputs have been persisted into the Fiji column.  Checks that the types and
   * values match.
   * @param inputs to check against
   * @param column column that the inputs are stored in
   * @tparam T expected return type of value in HBase
   */
  def verify[T](inputs: Iterable[(EntityId, T)],
                column: FijiColumnName,
                verifier: (T, T) => Unit): Unit = {
    inputs.foreach { input: (EntityId, T) =>
      val (eid, value) = input
      val retrieved: T = getValue(eid.components.head.toString, column)
      verifier(value, retrieved)
    }
  }

  def valueVerifier[T](input: T, retrieved: T): Unit = {
    assert(input === retrieved)
  }

  def nullVerifier(input: Any, retrieved: Any): Unit = {
    assert(input === null)
    assert(retrieved === null)
  }

  def arrayVerifier[T](input: T, retrieved: T): Unit = {
    assert(retrieved.isInstanceOf[GenericData.Array[_]])
    val ret = JavaConversions.JListWrapper(retrieved.asInstanceOf[GenericData.Array[_]]).toSeq
    assert(input.asInstanceOf[Iterable[_]].toSeq === ret)
  }

  def fixedVerifier[T](input: T, retrieved: T): Unit = {
    assert(retrieved.isInstanceOf[Fixed])
    assert(input === retrieved.asInstanceOf[Fixed].bytes())
  }

  def enumVerifier[T](schema: Schema)(input: T, retrieved: T): Unit = {
    assert(retrieved.isInstanceOf[GenericData.EnumSymbol])
    assert(
      retrieved.asInstanceOf[GenericData.EnumSymbol] ===
        new GenericData().createEnum(input.toString, schema))
  }

  /**
   * Write provided values with express into an HBase column with options as specified in output,
   * and verify that the values have been persisted correctly.
   * @param values to test
   * @param output options to write with
   * @tparam T type of values to write
   * @return
   */
  def testWrite[T](values: Iterable[T],
                   output: ColumnOutputSpec,
                   verifier: (T, T) => Unit = valueVerifier _) {
    val outputSource = FijiOutput.builder
        .withTableURI(uri)
        .withColumnSpecs(Map('value -> output))
        .build
    val inputs = eids.zip(values)
    expressWrite(conf, new Fields("entityId", "value"), inputs, outputSource)
    verify(inputs, output.columnName, verifier)
  }

  test("A FijiJob can write to a counter column with a Writer schema spec.") {
    testWrite(longs, QualifiedColumnOutputSpec(family, counterColumn, Writer))
  }

  test("A FijiJob can write to a raw bytes column with a Writer schema spec.")    {
    testWrite(bytes, QualifiedColumnOutputSpec(family, rawColumn, Writer))
  }

  test("A FijiJob can write to an Avro null column with a Generic schema spec.") {
    testWrite(nulls, QualifiedColumnOutputSpec(family, nullColumn, Generic(nullSchema)),
        nullVerifier)
  }

  test("A FijiJob can write to an Avro null column with a Writer schema spec.") {
    testWrite(nulls, QualifiedColumnOutputSpec(family, nullColumn), nullVerifier)
  }

  test("A FijiJob can write to an Avro boolean column with a Generic schema spec.") {
    testWrite(booleans, QualifiedColumnOutputSpec(family, booleanColumn, Generic(booleanSchema)))
  }

  test("A FijiJob can write to an Avro boolean column with a Writer schema spec.") {
    testWrite(booleans, QualifiedColumnOutputSpec(family, booleanColumn, Writer))
  }

  test("A FijiJob can write to an Avro int column with a Generic schema spec.") {
    testWrite(ints, QualifiedColumnOutputSpec(family, intColumn, Generic(intSchema)))
  }

  test("A FijiJob can write to an Avro int column with a Writer schema spec.") {
    testWrite(ints, QualifiedColumnOutputSpec(family, intColumn, Writer))
  }

  test("A FijiJob can write to an Avro long column with a Generic schema spec.") {
    testWrite(longs, QualifiedColumnOutputSpec(family, longColumn, Generic(longSchema)))
  }

  test("A FijiJob can write to an Avro long column with a Writer schema spec.") {
    testWrite(longs, QualifiedColumnOutputSpec(family, longColumn, Writer))
  }

  test("A FijiJob can write ints to an Avro long column with an int schema.") {
    testWrite(ints, QualifiedColumnOutputSpec(family, longColumn, Generic(intSchema)))
  }

  test("A FijiJob can write to an Avro float column with a Generic schema spec.") {
    testWrite(floats, QualifiedColumnOutputSpec(family, floatColumn, Generic(floatSchema)))
  }

  test("A FijiJob can write to an Avro float column with a Writer schema spec.") {
    testWrite(floats, QualifiedColumnOutputSpec(family, floatColumn, Writer))
  }

  test("A FijiJob can write to an Avro double column with a Generic schema spec.") {
    testWrite(doubles, QualifiedColumnOutputSpec(family, doubleColumn, Generic(doubleSchema)))
  }

  test("A FijiJob can write to an Avro double column with a Writer schema spec.") {
    testWrite(doubles, QualifiedColumnOutputSpec(family, doubleColumn, Writer))
  }

  test("A FijiJob can write floats to an Avro double column with a float schema.") {
    testWrite(floats, QualifiedColumnOutputSpec(family, doubleColumn, Generic(intSchema)))
  }

  /** TODO: reenable when Schema-594 is fixed. */
  ignore("A FijiJob can write to an Avro bytes column with a Generic schema spec.") {
    testWrite(bytes, QualifiedColumnOutputSpec(family, bytesColumn, Generic(bytesSchema)))
  }

  /** TODO: reenable when Schema-594 is fixed. */
  ignore("A FijiJob can write to an Avro bytes column with a Writer schema spec.") {
    testWrite(bytes, QualifiedColumnOutputSpec(family, bytesColumn, Writer))
  }

  test("A FijiJob can write to an Avro string column with a Generic schema spec.") {
    testWrite(strings, QualifiedColumnOutputSpec(family, stringColumn, Generic(stringSchema)))
  }

  test("A FijiJob can write to an Avro string column with a Writer schema spec.") {
    testWrite(strings, QualifiedColumnOutputSpec(family, stringColumn, Writer))
  }

  test("A FijiJob can write to an Avro specific record column with a Generic schema spec.") {
    testWrite(specificRecords, QualifiedColumnOutputSpec(family, specificColumn,
        Generic(specificSchema)))
  }

  test("A FijiJob can write to an Avro specific record column with a Writer schema spec.") {
    testWrite(specificRecords, QualifiedColumnOutputSpec(family, specificColumn, Writer))
  }

  test("A FijiJob can write to a generic record column with a Generic schema spec.") {
    testWrite(genericRecords, QualifiedColumnOutputSpec(family, genericColumn,
        Generic(genericSchema)))
  }

  test("A FijiJob can write to a generic record column with a Writer schema spec.") {
    testWrite(genericRecords, QualifiedColumnOutputSpec(family, genericColumn, Writer))
  }

  test("A FijiJob can write to an enum column with a Generic schema spec.") {
    testWrite(enums, QualifiedColumnOutputSpec(family, enumColumn, Generic(enumSchema)))
  }

  test("A FijiJob can write to an enum column with a Writer schema spec.") {
    testWrite(enums, QualifiedColumnOutputSpec(family, enumColumn, Writer))
  }

  test("A FijiJob can write a string to an enum column with a Generic schema spec.") {
    testWrite(enumStrings, QualifiedColumnOutputSpec(family, enumColumn, Generic(enumSchema)),
        enumVerifier(enumSchema))
  }

  test("A FijiJob can write an avro array to an array column with a Generic schema spec.") {
    testWrite(avroArrays, QualifiedColumnOutputSpec(family, arrayColumn, Generic(arraySchema)))
  }

  test("A FijiJob can write an avro array to an array column with a Writer schema spec."){
    testWrite(avroArrays, QualifiedColumnOutputSpec(family, arrayColumn, Writer))
  }

  test("A FijiJob can write an Iterable to an array column with a Generic schema spec.") {
    testWrite(arrays, QualifiedColumnOutputSpec(family, arrayColumn, Generic(arraySchema)),
        arrayVerifier)
  }

  test("A FijiJob can write to a union column with a Generic schema spec.") {
    testWrite(unions, QualifiedColumnOutputSpec(family, unionColumn, Generic(unionSchema)))
  }

  test("A FijiJob can write to a union column with a Writer schema spec.") {
    testWrite(unions, QualifiedColumnOutputSpec(family, unionColumn, Writer))
  }

  test("A FijiJob can write to a fixed column with a Generic schema spec.") {
    testWrite(fixeds, QualifiedColumnOutputSpec(family, fixedColumn, Generic(fixedSchema)))
  }

  test("A FijiJob can write to a fixed column with a Writer schema spec.") {
    testWrite(fixeds, QualifiedColumnOutputSpec(family, fixedColumn, Writer))
  }

  test("A FijiJob can write a byte array to a fixed column with a Generic schema spec.") {
    testWrite(fixedByteArrays, QualifiedColumnOutputSpec(family, fixedColumn, Generic(fixedSchema)),
      fixedVerifier)
  }
}

object WriterSchemaSuite {
  /**
   * Writes inputs to outputSource with Express.
   * @param fs fields contained in the input tuples.
   * @param inputs contains tuples to write to HBase with Express.
   * @param outputSource FijiSource with options for how to write values to HBase.
   * @param setter necessary for some implicit shenanigans.  Don't explicitly pass in.
   * @tparam A type of values to be written.
   */
  def expressWrite[A](conf: Configuration,
                      fs: Fields,
                      inputs: Iterable[A],
                      outputSource: FijiSource)
                     (implicit setter: TupleSetter[A]): Boolean = {

    val argsWithMode = Mode.putMode(Hdfs(strict = true, conf), Args(Nil))
    new IdentityJob(fs, inputs, outputSource, argsWithMode).run
  }
}

// Must be its own top-level class for mystical serialization reasons
class IdentityJob[A](fs: Fields, inputs: Iterable[A], output: FijiSource, args: Args)
                    (implicit setter: TupleSetter[A]) extends FijiJob(args) {
  IterableSource(inputs, fs)(setter, implicitly[TupleConverter[A]]).write(output)
}
