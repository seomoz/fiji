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

import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.schema.FijiColumnName
import com.moz.fiji.schema.FijiURI

/**
 * Factory methods for constructing [[com.moz.fiji.express.flow.FijiSource]]s that will be used as
 * outputs of a FijiExpress flow.
 *
 * Example usage for the fields API:
 *
 * {{{
 *   FijiOutput.builder
 *       .withTableURI("fiji://localhost:2181/default/mytable")
 *       .withTimestampField('timestamp)
 *       .withColumns('column1 -> "info:column1", 'column2 -> "info:column2")
 *       .addColumnSpecs('column3 -> QualifiedColumnOutputSpec.builder
 *           .withColumn("info", "column3")
 *           .build)
 *       .build
 * }}}
 *
 * Example usage for the type safe API:
 * {{{
 *   // This will request all columns for the specified table.
 *   FijiOutput.typedSinkForTable("fiji://localhost:2181/default/mytable")
 * }}}
 *
 */
@ApiAudience.Public
@ApiStability.Stable
object FijiOutput {

  val DEFAULT_COLUMN_OUTPUT_SPECS: Map[Symbol, _ <: ColumnOutputSpec] = Map()

  /**
   * A factory method for instantiating [[com.moz.fiji.express.flow.FijiSource]]s used as sinks. This
   * method permits specifying the full range of read options for each column. If timestampField is
   * undefined, the current time will be used at the time of the write.
   *
   * @param tableUri that addresses a table in a Fiji instance.
   * @param timestampField is the name of a tuple field that will contain cell timestamps when the
   *     source is used for writing.
   * @param columns is a mapping specifying what column to write each field value to.
   * @return a source that can write tuple field values to columns of a Fiji table.
   */
  private[express] def apply(
      tableUri: String,
      timestampField: Option[Symbol],
      columns: Map[Symbol, _ <: ColumnOutputSpec]
  ): FijiSource = {
    new FijiSource(
        tableAddress = tableUri,
        timeRange = TimeRangeSpec.All,
        timestampField = timestampField,
        outputColumns = columns)
  }

  /**
   * Create a new instance of [[TypedFijiSource]] to be used as a sink
   *
   * @param tableUri uri for the table.
   * @tparam T is the type of the data written.
   * @return a source that is used to write tuple values to a Fiji table.
   */
  def typedSinkForTable[T <: Iterable[ExpressColumnOutput[_]]](
      tableUri: String
  ): TypedFijiSource[T] = {
    new TypedFijiSource[T](tableAddress = tableUri,  timeRange = TimeRangeSpec.All)
  }


  /**
   * Create a new instance of [[TypedFijiSource]] to be used as a sink
   *
   * @param fijiUri is the [[FijiURI]] for the table.
   * @tparam T is the type for the data being written.
   * @return a TypedFijiSource that is used to write tuple values to a Fiji table.
   */
  def typedSinkForTable[T <: Iterable[ExpressColumnOutput[_]]](
      fijiUri:FijiURI
  ): TypedFijiSource[T] = {
    typedSinkForTable(fijiUri.toString)
  }

  /**
   * Create a new empty FijiOutput.Builder.
   *
   * @return a new empty FijiOutput.Builder.
   */
  def builder: Builder = Builder()

  /**
   * Create a new FijiOutput.Builder as a copy of the given Builder.
   *
   * @param other Builder to copy.
   * @return a new FijiOutput.Builder as a copy of the given Builder.
   */
  def builder(other: Builder): Builder = Builder(other)

  /**
   * Builder for [[com.moz.fiji.express.flow.FijiSource]]s to be used as outputs.
   *
   * @param mTableURI string of the table to which to write.
   * @param mTimestampField flow Field from which to read the timestamp.
   * @param mColumnSpecs mapping from Field to output specification.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  final class Builder private(
      private[this] var mTableURI: Option[String],
      private[this] var mTimestampField: Option[Symbol],
      private[this] var mColumnSpecs: Option[Map[Symbol, _ <: ColumnOutputSpec]]
  ) {
    /** protects read and write access to private var fields. */
    private val monitor = new AnyRef

    /**
     * Configure the FijiSource to write to the table with the given URI.
     *
     * @param tableURI string of the table to which to write.
     * @return this builder.
     */
    def withTableURI(tableURI: String): Builder = monitor.synchronized {
      require(tableURI != null, "Table URI may not be null.")
      require(mTableURI.isEmpty, "Table URI already set to: " + mTableURI.get)
      mTableURI = Some(tableURI)
      this
    }

    /**
     * Configure the FijiSource to write to the table with the given URI.
     *
     * @param tableURI string of the table to which to write.
     * @return this builder.
     */
    def withTableURI(tableURI: FijiURI): Builder = withTableURI(tableURI.toString)

    /**
     * Get the output table URI from this builder.
     *
     * @return the output table URI from this builder.
     */
    def tableURI: Option[String] = monitor.synchronized(mTableURI)
    /**
     * Configure the FijiSource to write with the timestamp found in the given Field.
     *
     * @param timestampField whose value will be used as a timestamp when writing.
     * @return this builder.
     */
    def withTimestampField(timestampField: Symbol): Builder = monitor.synchronized {
      require(timestampField != null, "Timestamp field may not be null.")
      require(mTimestampField.isEmpty, "Timestamp field already set to: " + mTimestampField)
      mTimestampField = Some(timestampField)
      this
    }

    /**
     * Get the Field whose value will be used as a timestamp when writing.
     *
     * @return the Field whose value will be used as a timestamp when writing.
     */
    def timestampField: Option[Symbol] = monitor.synchronized(mTimestampField)

    /**
     * Configure the FijiSource to write values of the given Fields to the corresponding columns.
     *
     * @param columns mapping from Field to Fiji column where the value of the field will be
     *     written.
     * @return this builder.
     */
    def withColumns(columns: Map[Symbol, String]): Builder = monitor.synchronized {
      require(columns != null, "Column output specs may not be null.")
      require(columns.size == columns.values.toSet.size,
          "Column output specs may not contain duplicate columns, found: " + columns)
      require(mColumnSpecs.isEmpty, "Column output specs already set to: " + mColumnSpecs)
      mColumnSpecs = Some(columns.mapValues { QualifiedColumnOutputSpec.fromColumnName })
      this
    }

    /**
     * Configure the FijiSource to write values of the given fields to the corresponding columns.
     *
     * @param columns mapping from Field to Fiji column where the value of the field will be
     *     written.
     * @return this builder.
     */
    def withColumns(columns: (Symbol, String)*): Builder = {
      withColumns(columns.toMap)
    }

    /**
     * Configure the FijiSource to write values of the given fields to the corresponding columns.
     *
     * @param columns mapping from Field to Fiji column where the value of the field will be
     *     written.
     * @return this builder.
     */
    def addColumns(columns: Map[Symbol, String]): Builder = monitor.synchronized {
      require(columns != null, "Column output specs may not be null.")
      require(columns.size == columns.values.toSet.size,
        "Column output specs may not contain duplicate columns, found: " + columns)
      mColumnSpecs match {
        case Some(cs) => {
          val colsList: List[String] = columns.values.toList
          val duplicateFieldOrColumn: Boolean = cs.exists {
            case (field, column) => columns.contains(field) || colsList.contains(column)
          }
          require(!duplicateFieldOrColumn, ("Column output specs already set to: %s May "
              + "not add duplicate Fields or columns.").format(mColumnSpecs.get))
          mColumnSpecs = Some(cs ++ columns.mapValues(QualifiedColumnOutputSpec.fromColumnName))
        }
        case None => {
          mColumnSpecs = Some(columns.mapValues(QualifiedColumnOutputSpec.fromColumnName))
        }
      }
      this
    }

    /**
     * Configure the FijiSource to write values of the given fields to the corresponding columns.
     *
     * @param columns mapping from Field to Fiji column where the value of the field will be
     *     written.
     * @return this builder.
     */
    def addColumns(columns: (Symbol, String)*): Builder = {
      addColumns(columns.toMap)
    }

    /**
     * Configure the FijiSource to write values of the given fields to the corresponding columns.
     *
     * @param columnSpecs mapping from Field to output specifications for the value stored in that
     *     Field.
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
        case ColumnFamilyOutputSpec(family, qualifierSelector, _) => (family, qualifierSelector)
      }.toSet.size,
        "Column output specifications may not contain duplicate columns. Column family output "
          + "specifications are considered duplicate if the family and qualifier selector both "
          + "match, found: " + columnSpecs)

      // synchronize access to mColumnSpecs
      monitor.synchronized {
        require(mColumnSpecs.isEmpty,
          "Column output specifications already set to: " + mColumnSpecs.get)
        mColumnSpecs = Some(columnSpecs)
        this
      }
    }

    /**
     * Configure the FijiSource to write values of the given fields to the corresponding columns.
     *
     * @param columnSpecs mapping from Field to output specifications for the value stored in that
     *     Field.
     * @return this builder.
     */
    def withColumnSpecs(columnSpecs: (Symbol, _ <: ColumnOutputSpec)*): Builder = {
      withColumnSpecs(columnSpecs.toMap[Symbol, ColumnOutputSpec])
    }

    /**
     * Configure the FijiSource to write values of the given fields to the corresponding columns.
     *
     * @param columnSpecs mapping from Field to output specifications for the value stored in that
     *     Field.
     * @return this builder.
     */
    def addColumnSpecs(columnSpecs: Map[Symbol, _ <: ColumnOutputSpec]): Builder = {
      require(columnSpecs != null, "Column output specs may not be null.")
      val (qualified, families) = columnSpecs.values.partition {
        case _: QualifiedColumnOutputSpec => true
        case _: ColumnFamilyOutputSpec => false
      }
      require(qualified.size == qualified.map { _.columnName }.toSet.size,
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
     * Configure the FijiSource to write values of the given fields to the corresponding columns.
     *
     * @param columnSpecs mapping from Field to output specifications for the value stored in that
     *     Field.
     * @return this builder.
     */
    def addColumnSpecs(columnSpecs: (Symbol, _ <: ColumnOutputSpec)*): Builder = {
      addColumnSpecs(columnSpecs.toMap[Symbol, ColumnOutputSpec])
    }

    /**
     * Get the output specifications from this Builder.
     *
     * @return the output specifications from this Builder.
     */
    def columnSpecs: Option[Map[Symbol, _ <: ColumnOutputSpec]] = monitor.synchronized(mColumnSpecs)

    /**
     * Build a new FijiSource configured for output from the values stored in this Builder.
     *
     * @throws IllegalStateException if the builder is not in a valid state to be built.
     * @return a new FijiSource configured for output from the values stored in this Builder.
     */
    def build: FijiSource = monitor.synchronized {
      FijiOutput(
          mTableURI.getOrElse(throw new IllegalStateException("Table URI must be specified.")),
          mTimestampField,
          mColumnSpecs.getOrElse(DEFAULT_COLUMN_OUTPUT_SPECS)
      )
    }
  }

  /**
   * Companion object providing factory methods for creating new instances of
   * [[com.moz.fiji.express.flow.FijiOutput.Builder]].
   */
  @ApiAudience.Public
  @ApiStability.Stable
  object Builder {
    /**
     * Create a new empty FijiOutput.Builder.
     *
     * @return a new empty FijiOutput.Builder.
     */
    private[express] def apply(): Builder = new Builder(None, None, None)

    /**
     * Create a new FijiOutputBuilder as a copy of the given Builder.
     *
     * @param other Builder to copy.
     * @return a new FijiOutputBuilder as a copy of the given Builder.
     */
    private[express] def apply(other: Builder): Builder = other.monitor.synchronized {
      // synchronize to get consistent snapshot of other
      new Builder(other.tableURI, other.timestampField, other.columnSpecs)
    }
  }
}
