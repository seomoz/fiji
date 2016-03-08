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

package com.moz.fiji.express.flow.framework.hfile

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.annotations.Inheritance
import com.moz.fiji.express.flow.ColumnFamilyOutputSpec
import com.moz.fiji.express.flow.ColumnOutputSpec
import com.moz.fiji.express.flow.QualifiedColumnOutputSpec
import com.moz.fiji.schema.FijiColumnName

/**
 * Factory methods for constructing [[com.moz.fiji.express.flow.framework.hfile.HFileFijiSource]]s that
 * will be used as outputs of a Fijiexpress flow.
 *
 * {{{
 *   // Create an HFileFijiOutput that writes to the table named `mytable` putting timestamps in the
 *   // `'timestamps` field and writing the fields `'column1` and `'column2` to the columns
 *   // `info:column1` and `info:column2`. The resulting HFiles will be written to the "my_hfiles"
 *   // folder.
 *   HFileFijiOutput.builder
 *       .withTableURI("fiji://localhost:2181/default/mytable")
 *       .withHFileOutput("my_hfiles")
 *       .withTimestampField('timestamps)
 *       .withColumns('column1 -> "info:column1", 'column2 -> "info:column2")
 *       .build
 * }}}
 */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Sealed
object HFileFijiOutput {

  val TEMP_HFILE_OUTPUT_KEY = "fiji.tempHFileOutput"

  /**
   * Create a new empty HFileFijiOutput.Builder.
   *
   * @return a new empty HFileFijiOutput.Builder.
   */
  def builder: Builder = Builder()

  /**
   * Create a new HFileFijiOutput.Builder as a copy of the given Builder.
   *
   * @param other Builder to copy.
   * @return a new HFileFijiOutput.Builder as a copy of the given Builder.
   */
  def builder(other: Builder): Builder = Builder(other)

  /**
   * Builder for [[com.moz.fiji.express.flow.framework.hfile.HFileFijiSource]]s to be used as sinks.
   *
   * @param mTableURI string address of the table to which to write.
   * @param mHFileOutput path to the output file.
   * @param mTimestampField flow Field from which to read the timestamp.
   * @param mColumnSpecs mapping from Field to output specification.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  final class Builder private(
      private[this] var mTableURI: Option[String],
      private[this] var mHFileOutput: Option[String],
      private[this] var mTimestampField: Option[Symbol],
      private[this] var mColumnSpecs: Option[Map[Symbol, ColumnOutputSpec]]
  ) {
    /** protects read and write access to private var fields. */
    private val monitor = new AnyRef

    /**
     * Get the output table URI from this builder.
     *
     * @return the output table URI from this builder.
     */
    def tableURI: Option[String] = monitor.synchronized(mTableURI)

    /**
     * Get the output file path where the HFile will be written.
     *
     * @return the output file path where the HFile will be written.
     */
    def hFileOutput: Option[String] = monitor.synchronized(mHFileOutput)

    /**
     * Get the Field whose value will be used as a timestamp when writing.
     *
     * @return the Field whose value will be used as a timestamp when writing.
     */
    def timestampField: Option[Symbol] = monitor.synchronized(mTimestampField)

    /**
     * Get the output specifications from this Builder.
     *
     * @return the output specifications from this Builder.
     */
    def columnSpecs: Option[Map[Symbol, ColumnOutputSpec]] = monitor.synchronized(mColumnSpecs)

    /**
     * Configure the HFileFijiSource to write an HFile compatible with the given table URI.
     *
     * @param tableURI string of the table for which to write HFiles.
     * @return this builder.
     */
    def withTableURI(tableURI: String): Builder = monitor.synchronized {
      require(tableURI != null, "Table URI may not be null.")
      require(mTableURI.isEmpty, "Output table URI already set to: " + mTableURI.get)
      mTableURI = Some(tableURI)
      this
    }

    /**
     * Configure the HFileFijiSource to write the HFile to the given file path.
     *
     * @param output path where the HFile will be written.
     * @return this builder.
     */
    def withHFileOutput(output: String): Builder = monitor.synchronized {
      require(output != null, "HFile output path may not be null.")
      require(mHFileOutput.isEmpty, "HFile output file already set to: " + mHFileOutput.get)
      mHFileOutput = Some(output)
      this
    }

    /**
     * Configure the HFileFijiSource to write values at the timestamp stored in the given tuple
     * Field.
     *
     * @param timestampField at whose value data will be written.
     * @return this builder.
     */
    def withTimestampField(timestampField: Symbol): Builder = monitor.synchronized {
      require(timestampField != null, "Timestamp field may not be null.")
      require(mTimestampField.isEmpty, "Timestamp field already set to: " + mTimestampField)
      mTimestampField = Some(timestampField)
      this
    }

    /**
     * Configure the HFileFijiSource to write the given tuple Field values to the associated
     * columns.
     *
     * @param columns mapping from tuple Fields to columns into which Field values will be written.
     * @return this builder.
     */
    def withColumns(columns: (Symbol, String)*): Builder = withColumns(columns.toMap)

    /**
     * Configure the HFileFijiSource to write the given tuple Field values to the associated
     * columns.
     *
     * @param columns mapping from tuple Fields to columns into which Field values will be written.
     * @return this builder.
     */
    def withColumns(columns: Map[Symbol, String]): Builder = withColumnSpecs(columns.mapValues {
      QualifiedColumnOutputSpec.fromColumnName
    })

    /**
     * Configure the HFileFijiSource to write the given tuple Field values to the associated
     * columns.
     *
     * @param columnSpecs mapping from tuple Fields to columns into which Field values will be
     *     written.
     * @return this builder.
     */
    def withColumnSpecs(columnSpecs: (Symbol, _ <: ColumnOutputSpec)*): Builder =
        withColumnSpecs(columnSpecs.toMap[Symbol, ColumnOutputSpec])

    /**
     * Configure the HFileFijiSource to write the given tuple Field values to the associated
     * columns.
     *
     * @param columnSpecs mapping from tuple Fields to columns into which Field values will be
     *     written.
     * @return this builder.
     */
    def withColumnSpecs(columnSpecs: Map[Symbol, _ <: ColumnOutputSpec]): Builder = {
      require(columnSpecs != null, "Column output specs may not be null.")
      val (qualified, families) = columnSpecs.values.partition {
        case _: QualifiedColumnOutputSpec => true
        case _: ColumnFamilyOutputSpec => false
      }
      require(qualified.size == qualified.map(_.columnName).toSet.size,
          "Column output specifications may not contain duplicate columns, found: " + columnSpecs)
      require(families.size == families.map {
            case ColumnFamilyOutputSpec(family, qualifierSelector, _) =>
                (family, qualifierSelector)
          }.toSet.size,
          "Column output specifications may not contain duplicate columns. Column family output "
          + "specifications are considered duplicate if the family and qualifier selector both "
          + "match, found: " + columnSpecs)

      // synchronize access to mColumnSpecs
      monitor.synchronized {
        require(mColumnSpecs.isEmpty,
            "Column output specifications already set to: " + mColumnSpecs.get)
        mColumnSpecs = Some(columnSpecs)
      }
      this
    }

    /**
     * Configure the HFileFijiSource to write the given tuple Field values to the associated
     * columns.
     *
     * @param columns mapping from tuple Fields to columns into which Field values will be written.
     * @return this builder.
     */
    def addColumns(columns: (Symbol, String)*): Builder = addColumns(columns.toMap)

    /**
     * Configure the HFileFijiSource to write the given tuple Field values to the associated
     * columns.
     *
     * @param columns mapping from tuple Fields to columns into which Field values will be written.
     * @return this builder.
     */
    def addColumns(columns: Map[Symbol, String]): Builder = addColumnSpecs(columns.mapValues {
      QualifiedColumnOutputSpec.fromColumnName
    })

    /**
     * Configure the HFileFijiSource to write the given tuple Field values to the associated
     * columns.
     *
     * @param columnSpecs mapping from tuple Fields to columns into which Field values will be
     *     written.
     * @return this builder.
     */
    def addColumnSpecs(columnSpecs: (Symbol, _ <: ColumnOutputSpec)*): Builder =
        addColumnSpecs(columnSpecs.toMap[Symbol, ColumnOutputSpec])

    /**
     * Configure the HFileFijiSource to write the given tuple Field values to the associated
     * columns.
     *
     * @param columnSpecs mapping from tuple Fields to columns into which Field values will be
     *     written.
     * @return this builder.
     */
    def addColumnSpecs(columnSpecs: Map[Symbol, _ <: ColumnOutputSpec]): Builder = {
      require(columnSpecs != null, "Column output specs may not be null.")
      val (qualified, families) = columnSpecs.values.partition {
        case qcos: QualifiedColumnOutputSpec => true
        case cfos: ColumnFamilyOutputSpec => false
      }
      require(qualified.size == qualified.map(_.columnName).toSet.size,
        "Column output specifications may not contain duplicate columns, found: " + columnSpecs)
      require(families.size == families.map {
        case ColumnFamilyOutputSpec(family, qualifierSelector, _) => (family, qualifierSelector)
      }.toSet.size,
        "Column output specifications may not contain duplicate columns. Column family output "
            + "specifications are considered duplicate if the family and qualifier selector both "
            + "match, found: " + columnSpecs)
      // synchronize access to mColumnSpecs
      monitor.synchronized {
        mColumnSpecs match {
          case Some(cs) => {
            val colsList: List[FijiColumnName] = columnSpecs.values.toList.map { _.columnName }
            val duplicateFieldOrColumn = cs.exists { case (field, spec) =>
              columnSpecs.contains(field) || colsList.contains(spec.columnName)
            }
            require(!duplicateFieldOrColumn, ("Column output specifications already set to: %s May "
                + "not add duplicate Fields or columns.").format(mColumnSpecs.get))
            mColumnSpecs = Some(cs ++ columnSpecs)
          }
          case None => mColumnSpecs = Some(columnSpecs)
        }
      }
      this
    }

    /**
     * Build a new HFileFijiSource from the values stored in this Builder.
     *
     * @throws IllegalStateException if the builder is not in a valid state to be built.
     * @return a new HFileFijiSource from the values stored in this Builder.
     */
    def build: HFileFijiSource = monitor.synchronized {
      HFileFijiOutput(
        mTableURI.getOrElse(throw new IllegalStateException("Table URI must be specified.")),
        mHFileOutput.getOrElse(throw new IllegalStateException("HFile output must be specified.")),
        mTimestampField,
        mColumnSpecs.getOrElse(
            throw new IllegalStateException("Column output specs must be specified.")))
    }
  }

  /**
   * Companion object providing factory methods for creating new
   * [[com.moz.fiji.express.flow.framework.hfile.HFileFijiOutput.Builder]] instances.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  object Builder {
    /**
     * Create a new empty Builder instance.
     *
     * @return a new empty Builder instance.
     */
    private[express] def apply(): Builder = new Builder(None, None, None, None)

    /**
     * Create a new Builder instance as a copy of the given Builder.
     *
     * @param other Builder to copy.
     * @return a new Builder instance as a copy of the given Builder.
     */
    private[express] def apply(other: Builder): Builder = other.monitor.synchronized {
      // synchronize to get consistent snapshot of other
      new Builder(other.tableURI, other.hFileOutput, other.timestampField, other.columnSpecs)
    }
  }

  /**
   * A factory method for instantiating [[com.moz.fiji.express.flow.framework.hfile.HFileFijiSource]]s
   * used as sinks.
   *
   * @param tableURI that addresses a table in a Fiji instance.
   * @param hFileOutput is the location where the resulting HFiles will be placed.
   * @param timestampField is the name of a tuple field that will contain cell timestamps when the
   *     source is used for writing.
   * @param columns is a mapping specifying what column to which to write each field value.
   * @return a new HFileFijiSource that writes tuple field values to an HFile for a Fiji table.
   */
  private[express] def apply(
      tableURI: String,
      hFileOutput: String,
      timestampField: Option[Symbol] = None,
      columns: Map[Symbol, _ <: ColumnOutputSpec]
  ): HFileFijiSource = {
    new HFileFijiSource(
      tableAddress = tableURI,
      hFileOutput = hFileOutput,
      timestampField = timestampField,
      columns = columns)
  }
}
