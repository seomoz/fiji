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
 * inputs to a FijiExpress flow.
 *
 * Example usage:
 *
 * {{{
 *   val column3 = QualifiedColumnInputSpec.builder
 *       .withColumn("info", "column3")
 *       .withSchemaSpec(DefaultReader)
 *       .build
 *
 *   //Fields API
 *   FijiInput.builder
 *       .withTableURI("fiji://localhost:2181/default/mytable")
 *       .withTimeRangeSpec(TimeRangeSpec.Between(5, 10))
 *       .withColumns("info:column1" -> 'column1, "info:column2" -> 'column2)
 *       .addColumnSpecs(column3 -> 'column3)
 *       // Selects a 30% sample of data between startEid and endEid.
 *       .withRowRangeSpec(RowRangeSpec.Between(startEid, endEid)
 *       .withRowFilterSpec(RowFilterSpec.Random(0.3F))
 *       .build
 *
 *   //Typed API
 *   FijiInput.typedBuilder
 *       .withTableURI("fiji://localhost:2181/default/mytable")
 *       .withColumns("info:column1", "info:column2")
 *       .addColumnSpecs(column3)
 *       .withRowRangeSpec(RowRangeSpec.Between(startEid, endEid)
 *       .withRowFilterSpec(RowFilterSpec.Random(0.3F))
 *       .build
 * }}}
 *
 * Note: Columns containing no values will be replaced with an empty sequence unless all requested
 *     columns are empty in which case the entire row will be skipped.
 */
@ApiAudience.Public
@ApiStability.Stable
object FijiInput {
  /** Default time range for FijiSource */
  private val DEFAULT_TIME_RANGE: TimeRangeSpec = TimeRangeSpec.All

  /**
   * Create a new empty FijiInput.Builder.
   *
   * @return a new empty FijiInput.Builder.
   */
  def builder: Builder = Builder()

  /**
   * Create a new FijiInput.Builder as a copy of the given Builder.
   *
   * @param other Builder to copy.
   * @return a new FijiInput.Builder as a copy of the given Builder.
   */
  def builder(other: Builder): Builder = Builder(other)

  /**
   * Create an empty FijiInput.TypedBuilder.
   *
   * @return an empty FijiInput.TypedBuilder
   */
  def typedBuilder: TypedBuilder = TypedBuilder()

  /**
   * Create a new FijiInput.TypedBuilder as a copy of the given TypedBuilder.
   *
   * @param other TypedBuilder to copy.
   * @return a new FijiInput.TypedBuilder as a copy of the given TypedBuilder.
   */
  def typedBuilder(other:TypedBuilder): TypedBuilder = TypedBuilder(other)

  /**
   * Builder for [[com.moz.fiji.express.flow.FijiSource]]s to be used as inputs.
   *
   * @param mTableURI string of the table from which to read.
   * @param mTimeRange from which to read values.
   * @param mColumnSpecs specification of columns from which to read.
   * @param mRowRangeSpec rows from which to read.
   * @param mRowFilterSpec filters used to read.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  final class Builder private(
      private[this] var mTableURI: Option[String],
      private[this] var mTimeRange: Option[TimeRangeSpec],
      private[this] var mColumnSpecs: Option[Map[_ <: ColumnInputSpec, Symbol]],
      private[this] var mRowRangeSpec: Option[RowRangeSpec],
      private[this] var mRowFilterSpec: Option[RowFilterSpec]
  ) {
    /** protects read and write access to private var fields. */
    private val monitor = new AnyRef

    /**
     * Get the Fiji URI of the table from which to read from this Builder.
     *
     * @return the Fiji URI of the table from which to read from this Builder.
     */
    def tableURI: Option[String] = monitor.synchronized(mTableURI)

    /**
     * Get the input time range specification from this Builder.
     *
     * @return the input time range specification from this Builder.
     */
    def timeRange: Option[TimeRangeSpec] = monitor.synchronized(mTimeRange)

    /**
     * Get the input specifications from this Builder.
     *
     * @return the input specifications from this Builder.
     */
    def columnSpecs: Option[Map[_ <: ColumnInputSpec, Symbol]] = monitor.synchronized(mColumnSpecs)

    /**
     * Get the input row range specification from this Builder.
     *
     * @return the input row range specification from this Builder.
     */
    def rowRangeSpec: Option[RowRangeSpec] = monitor.synchronized(mRowRangeSpec)

    /**
     * Get the input row filter specification from this Builder.
     *
     * @return the input row filter specification from this Builder.
     */
    def rowFilterSpec: Option[RowFilterSpec] = monitor.synchronized(mRowFilterSpec)

    /**
     * Configure the FijiSource to read values from the table with the given Fiji URI.
     *
     * @param tableURI of the table from which to read.
     * @return this builder.
     */
    def withTableURI(tableURI: String): Builder = monitor.synchronized {
      require(tableURI != null, "Table URI may not be null.")
      require(mTableURI.isEmpty, "Table URI already set to: " + mTableURI.get)
      mTableURI = Some(tableURI)
      this
    }

    /**
     * Configure the FijiSource to read values from the table with the given Fiji URI.
     *
     * @param tableURI of the table from which to read.
     * @return this builder.
     */
    def withTableURI(tableURI: FijiURI): Builder = withTableURI(tableURI.toString)

    /**
     * Configure the FijiSource to read values from the given range of input times.
     *
     * @param timeRangeSpec specification of times from which to read.
     * @return this builder.
     */
    def withTimeRangeSpec(timeRangeSpec: TimeRangeSpec): Builder = monitor.synchronized {
      require(timeRangeSpec != null, "Time range may not be null.")
      require(mTimeRange.isEmpty, "Time range already set to: " + mTimeRange.get)
      mTimeRange = Some(timeRangeSpec)
      this
    }

    /**
     * Configure the FijiSource to read values from the given columns into the corresponding fields.
     *
     * @param columns mapping from column inputs to fields which will hold the values from those
     *     columns.
     * @return this builder.
     */
    def withColumns(columns: (String, Symbol)*): Builder = withColumns(columns.toMap)

    /**
     * Configure the FijiSource to read values from the given columns into the corresponding fields.
     *
     * @param columns mapping from column inputs to fields which will hold the values from those
     *     columns.
     * @return this builder.
     */
    def withColumns(columns: Map[String, Symbol]): Builder =
        withColumnSpecs(columns.map { Builder.columnToSpec })

    /**
     * Configure the FijiSource to read values from the given columns into the corresponding fields.
     *
     * @param columns mapping from column inputs to fields which will hold the values from those
     *     columns.
     * @return this builder.
     */
    def addColumns(columns: (String, Symbol)*): Builder = addColumns(columns.toMap)

    /**
     * Configure the FijiSource to read values from the given columns into the corresponding fields.
     *
     * @param columns mapping from column inputs to fields which will hold the values from those
     *     columns.
     * @return this builder.
     */
    def addColumns(columns: Map[String, Symbol]): Builder =
        addColumnSpecs(columns.map { Builder.columnToSpec })

    /**
     * Configure the FijiSource to read values from the given columns into the corresponding fields.
     *
     * @param columnSpecs mapping from column inputs to fields which will hold the values from those
     *     columns.
     * @return this builder.
     */
    def withColumnSpecs(columnSpecs: (_ <: ColumnInputSpec, Symbol)*): Builder =
        withColumnSpecs(columnSpecs.toMap[ColumnInputSpec, Symbol])

    /**
     * Configure the FijiSource to read values from the given columns into the corresponding fields.
     *
     * @param columnSpecs mapping from column inputs to fields which will hold the values from those
     *     columns.
     * @return this builder.
     */
    def addColumnSpecs(columnSpecs: (_ <: ColumnInputSpec, Symbol)*): Builder =
        addColumnSpecs(columnSpecs.toMap[ColumnInputSpec, Symbol])

    /**
     * Configure the FijiSource to read values from the given columns into the corresponding fields.
     *
     * @param columnSpecs mapping from column inputs to fields which will hold the values from those
     *     columns.
     * @return this builder.
     */
    def withColumnSpecs(columnSpecs: Map[_ <: ColumnInputSpec, Symbol]): Builder = {
      require(columnSpecs != null, "Column input specs may not be null.")
      require(columnSpecs.size == columnSpecs.values.toSet.size,
        "Column input specs may not include duplicate Fields. found: " + columnSpecs)
      monitor.synchronized {
        require(mColumnSpecs.isEmpty, "Column input specs already set to: " + mColumnSpecs.get)
        mColumnSpecs = Some(columnSpecs)
      }
      this
    }

    /**
     * Configure the FijiSource to read values from the given columns into the corresponding fields.
     *
     * @param columnSpecs mapping from column inputs to fields which will hold the values from those
     *     columns.
     * @return this builder.
     */
    def addColumnSpecs(columnSpecs: Map[_ <: ColumnInputSpec, Symbol]): Builder = {
      require(columnSpecs != null, "Column input specs may not be null.")
      require(columnSpecs.size == columnSpecs.values.toSet.size,
        "Column input specs may not include duplicate Fields. found: " + columnSpecs)
      monitor.synchronized {
        mColumnSpecs match {
          case Some(cs) => {
            val symbols: List[Symbol] = columnSpecs.values.toList
            val duplicateField: Boolean = cs.toIterable.exists { entry: (ColumnInputSpec, Symbol) =>
              val (_, field) = entry
              symbols.contains(field)
            }
            require(!duplicateField, ("Column input specs already set to: %s May not add duplicate "
                + "Fields.").format(mColumnSpecs.get))
            mColumnSpecs = Some(cs ++ columnSpecs)
          }
          case None => mColumnSpecs = Some(columnSpecs)
        }
      }
      this
    }

    /**
     * Configure the FijiSource to traverse rows within the requested row range specification.
     *
     * @param rowRangeSpec requested range for rows.
     * @return this builder.
     */
    def withRowRangeSpec(rowRangeSpec: RowRangeSpec): Builder = monitor.synchronized {
      require(rowRangeSpec != null, "Row range spec may not be null.")
      require(mRowRangeSpec.isEmpty, "Row range spec already set to: " + mRowRangeSpec.get)
      mRowRangeSpec = Some(rowRangeSpec)
      this
    }

    /**
     * Configure the FijiSource to traverse rows with the requested row filter specification.
     *
     * @param rowFilterSpec requested row filter.
     * @return this builder.
     */
    def withRowFilterSpec(rowFilterSpec: RowFilterSpec): Builder = monitor.synchronized {
      require(rowFilterSpec != null, "Row filter spec may not be null.")
      require(mRowFilterSpec.isEmpty, "Row filter spec already set to: " + mRowFilterSpec.get)
      mRowFilterSpec = Some(rowFilterSpec)
      this
    }

    /**
     * Build a new FijiSource configured for input from the values stored in this Builder.
     *
     * @throws IllegalStateException if the builder is not in a valid state to be built.
     * @return a new FijiSource configured for input from the values stored in this Builder.
     */
    def build: FijiSource = monitor.synchronized {
      FijiInput(
          tableURI.getOrElse(throw new IllegalStateException("Table URI must be specified.")),
          timeRange.getOrElse(DEFAULT_TIME_RANGE),
          columnSpecs.getOrElse(
              throw new IllegalStateException("Column input specs must be specified.")),
          rowRangeSpec.getOrElse(RowRangeSpec.All),
          rowFilterSpec.getOrElse(RowFilterSpec.NoFilter))
    }
  }

  /**
   * Builder for [[TypedFijiSource]]'s to be used as inputs.
   *
   * @param mTableURI string of the table from which to read.
   * @param mTimeRange from which to read values.
   * @param mColumnSpecs is the list of specification of columns from which to read.
   * @param mRowRangeSpec rows from which to read.
   * @param mRowFilterSpec filters used to read.
   */
  @ApiAudience.Public
  @ApiStability.Evolving
  final class TypedBuilder private(
      private[this] var mTableURI: Option[String],
      private[this] var mTimeRange: Option[TimeRangeSpec],
      private[this] var mColumnSpecs: Option[List[_ <: ColumnInputSpec]],
      private[this] var mRowRangeSpec: Option[RowRangeSpec],
      private[this] var mRowFilterSpec: Option[RowFilterSpec]
  ) {

    /** protects read and write access to private var fields. */
    private val monitor = new AnyRef

    /**
     * Get the Fiji URI of the table from which to read from this TypedBuilder.
     *
     * @return the Fiji URI of the table from which to read from this TypedBuilder.
     */
    def tableURI: Option[String] = monitor.synchronized(mTableURI)

    /**
     * Get the input time range specification from this TypedBuilder.
     *
     * @return the input time range specification from this TypedBuilder.
     */
    def timeRange: Option[TimeRangeSpec] = monitor.synchronized(mTimeRange)

    /**
     * Get the input specifications from this TypedBuilder.
     *
     * @return the input specifications from this TypedBuilder.
     */
    def columnSpecs: Option[List[_ <: ColumnInputSpec]] = monitor.synchronized(mColumnSpecs)

    /**
     * Get the input row range specification from this TypedBuilder.
     *
     * @return the input row range specification from this TypedBuilder.
     */
    def rowRangeSpec: Option[RowRangeSpec] = monitor.synchronized(mRowRangeSpec)

    /**
     * Get the input row filter specification from this TypedBuilder.
     *
     * @return the input row filter specification from this TypedBuilder.
     */
    def rowFilterSpec: Option[RowFilterSpec] = monitor.synchronized(mRowFilterSpec)

    /**
     * Configure the [[TypedFijiSource]] to read values from the table with the given Fiji URI.
     *
     * @param tableURI of the table from which to read.
     * @return this TypedBuilder.
     */
    def withTableURI(tableURI: String): TypedBuilder = monitor.synchronized {
      require(tableURI != null, "Table URI may not be null.")
      require(mTableURI.isEmpty, "Table URI already set to: " + mTableURI.get)
      mTableURI = Some(tableURI)
      this
    }

    /**
     * Configure the [[TypedFijiSource]] to read values from the table with the given Fiji URI.
     *
     * @param tableURI of the table from which to read.
     * @return this TypedBuilder.
     */
    def withTableURI(tableURI: FijiURI): TypedBuilder = withTableURI(tableURI.toString)

    /**
     * Configure the [[TypedFijiSource]] to read values from the given range of input times.
     *
     * @param timeRangeSpec specification of times from which to read.
     * @return this TypedBuilder.
     */
    def withTimeRangeSpec(timeRangeSpec: TimeRangeSpec): TypedBuilder = monitor.synchronized {
      require(timeRangeSpec != null, "Time range may not be null.")
      require(mTimeRange.isEmpty, "Time range already set to: " + mTimeRange.get)
      mTimeRange = Some(timeRangeSpec)
      this
    }

    /**
     * Configure the [[TypedFijiSource]] to read values from the given columns into the
     * corresponding fields.
     *
     * @param columns mapping from column inputs to fields which will hold the values from those
     *     columns.
     * @return this TypedBuilder.
     */
    def withColumns(columns: (String)*): TypedBuilder = withColumns(columns.toList)

    /**
     * Configure the [[TypedFijiSource]] to read values from the given columns into the
     * corresponding fields.
     *
     * @param columns mapping from column inputs to fields which will hold the values from those
     *     columns.
     * @return this TypedBuilder.
     */
    def withColumns(columns: List[String]): TypedBuilder =
        withColumnSpecs(columns.map { TypedBuilder.columnToSpec })

    /**
     * Configure the [[TypedFijiSource]] to read values from the given columns into the
     * corresponding fields.
     *
     * @param columns mapping from column inputs to fields which will hold the values from those
     *     columns.
     * @return this TypedBuilder.
     */
    def addColumns(columns: String *): TypedBuilder = addColumns(columns.toList)

    /**
     * Configure the [[TypedFijiSource]] to read values from the given columns into the
     * corresponding fields.
     *
     * @param columns mapping from column inputs to fields which will hold the values from those
     *     columns.
     * @return this TypedBuilder.
     */
    def addColumns(columns: List[String]): TypedBuilder =
        addColumnSpecs(columns.map { TypedBuilder.columnToSpec })

    /**
     * Configure the [[TypedFijiSource]] to read values from the given columns into the
     * corresponding fields.
     *
     * @param columnSpecs mapping from column inputs to fields which will hold the values from those
     *     columns.
     * @return this TypedBuilder.
     */
    def withColumnSpecs(columnSpecs: (ColumnInputSpec)*): TypedBuilder =
        withColumnSpecs(columnSpecs.toList)

    /**
     * Configure the [[TypedFijiSource]] to read values from the given columns into the
     * corresponding fields.
     *
     * @param columnSpecs mapping from column inputs to fields which will hold the values from those
     *     columns.
     * @return this TypedBuilder.
     */
    def addColumnSpecs(columnSpecs: (ColumnInputSpec)*): TypedBuilder =
        addColumnSpecs(columnSpecs.toList)

    /**
     * Configure the [[TypedFijiSource]] to read values from the given columns into the
     * corresponding fields.
     *
     * @param columnSpecs mapping from column inputs to fields which will hold the values from those
     *     columns.
     * @return this TypedBuilder.
     */
    def withColumnSpecs(columnSpecs: List[ColumnInputSpec]): TypedBuilder = {
      require(columnSpecs != null, "Column input specs may not be null.")
      require(columnSpecs.size == columnSpecs.toSet.size,
          "Column input specs may not include duplicate Fields. found: " + columnSpecs)
      monitor.synchronized {
        require(mColumnSpecs.isEmpty, "Column input specs already set to: " + mColumnSpecs.get)
        mColumnSpecs = Some(columnSpecs)
      }
      this
    }

    /**
     * Configure the [[TypedFijiSource]] to read values from the given columns into the
     * corresponding fields.
     *
     * @param columnSpecs mapping from column inputs to fields which will hold the values from those
     *     columns.
     * @return this TypedBuilder.
     */
    def addColumnSpecs(columnSpecs: List[_ <: ColumnInputSpec]): TypedBuilder = {
      require(columnSpecs != null, "Column input specs may not be null.")
      require(columnSpecs.size == columnSpecs.toSet.size,
          "Column input specs may not include duplicate Fields. found: " + columnSpecs)
      monitor.synchronized {
        mColumnSpecs match {
          case Some(cs) =>  mColumnSpecs = Some(cs ++ columnSpecs)
          case None => mColumnSpecs = Some(columnSpecs)
        }
      }
      this
    }

    /**
     * Configure the [[TypedFijiSource]] to traverse rows within the requested row range
     * specification.
     *
     * @param rowRangeSpec requested range for rows.
     * @return this TypedBuilder
     */
    def withRowRangeSpec(rowRangeSpec: RowRangeSpec): TypedBuilder = monitor.synchronized {
      require(rowRangeSpec != null, "Row range spec may not be null.")
      require(mRowRangeSpec.isEmpty, "Row range spec already set to: " + mRowRangeSpec.get)
      mRowRangeSpec = Some(rowRangeSpec)
      this
    }

    /**
     * Configure the [[TypedFijiSource]] to traverse rows with the requested row filter
     * specification.
     *
     * @param rowFilterSpec requested row filter.
     * @return this builder.
     */
    def withRowFilterSpec(rowFilterSpec: RowFilterSpec): TypedBuilder = monitor.synchronized {
      require(rowFilterSpec != null, "Row filter spec may not be null.")
      require(mRowFilterSpec.isEmpty, "Row filter spec already set to: " + mRowFilterSpec.get)
      mRowFilterSpec = Some(rowFilterSpec)
      this
    }

    /**
     * Build a new [[TypedFijiSource]] configured for input from the values stored in this
     * TypedBuilder.
     *
     * @throws IllegalStateException if the builder is not in a valid state to be built.
     * @return a new TypedFijiSource configured for input from the values stored in this
     *         TypedBuilder.
     */
    def build: TypedFijiSource[ExpressResult] = monitor.synchronized {
      FijiInput.typedFijiSource(
          tableURI.getOrElse(throw new IllegalStateException("Table URI must be specified.")),
          timeRange.getOrElse(DEFAULT_TIME_RANGE),
          columnSpecs.getOrElse(
              throw new IllegalStateException("Column input specs must be specified.")),
          rowRangeSpec.getOrElse(RowRangeSpec.All),
          rowFilterSpec.getOrElse(RowFilterSpec.NoFilter))
    }
  }


  /**
   * Companion object providing utility methods and factory methods for creating new instances of
   * [[com.moz.fiji.express.flow.FijiInput.Builder]].
   */
  @ApiAudience.Public
  @ApiStability.Stable
  object Builder {

    /**
     * Create a new empty Builder.
     *
     * @return a new empty Builder.
     */
    def apply(): Builder = new Builder(None, None, None, None, None)

    /**
     * Create a new Builder as a copy of the given Builder.
     *
     * @param other Builder to copy.
     * @return a new Builder as a copy of the given Builder.
     */
    def apply(other: Builder): Builder = other.monitor.synchronized {
      // synchronize to get consistent snapshot of other
      new Builder(
          other.tableURI,
          other.timeRange,
          other.columnSpecs,
          other.rowRangeSpec,
          other.rowFilterSpec)
    }

    /**
     * Converts a column -> Field mapping to a ColumnInputSpec -> Field mapping.
     *
     * @param pair column to Field binding.
     * @return ColumnInputSpec to Field binding.
     */
    private def columnToSpec(pair: (String, Symbol)): (_ <: ColumnInputSpec, Symbol) = {
      val (column, field) = pair
      val colName: FijiColumnName = FijiColumnName.create(column)
      if (colName.isFullyQualified) {
        (QualifiedColumnInputSpec(colName.getFamily, colName.getQualifier), field)
      } else {
        (ColumnFamilyInputSpec(colName.getFamily), field)
      }
    }
  }

  /**
   * Companion object providing utility methods and factory methods for creating new instances of
   * [[com.moz.fiji.express.flow.FijiInput.TypedBuilder]].
   */
  object TypedBuilder {

    /**
     * Create a new empty TypedBuilder.
     *
     * @return a new empty TypedBuilder.
     */
    def apply(): TypedBuilder = new TypedBuilder(None, None, None, None, None)

    /**
     * Create a new TypedBuilder as a copy of the given TypedBuilder.
     *
     * @param other TypedBuilder to copy.
     * @return a new TypedBuilder as a copy of the given TypedBuilder.
     */
    def apply(other: TypedBuilder): TypedBuilder = other.monitor.synchronized {
      // synchronize to get consistent snapshot of other
      new TypedBuilder(
          other.tableURI,
          other.timeRange,
          other.columnSpecs,
          other.rowRangeSpec,
          other.rowFilterSpec)
    }

    /**
     * Converts a string identifying a column to a ColumnInputSpec.
     *
     * @param columnNameString is the string name
     * @return a ColumnInputSpec.
     */
    private def columnToSpec(columnNameString: String): (ColumnInputSpec) = {
      val (column) = columnNameString
      val colName: FijiColumnName = FijiColumnName.create(column)
      if (colName.isFullyQualified) {
        QualifiedColumnInputSpec(colName.getFamily, colName.getQualifier)
      } else {
        ColumnFamilyInputSpec(colName.getFamily)
      }
    }
  }

  /**
   * A factory method for creating a FijiSource.
   *
   * @param tableUri addressing a table in a Fiji instance.
   * @param timeRange that cells must fall into to be retrieved.
   * @param columns are a series of pairs mapping column input specs to tuple field names.
   *     Columns are specified as "family:qualifier" or, in the case of a column family input spec,
   *     simply "family".
   * @param rowRangeSpec the specification for which row interval to scan
   * @param rowFilterSpec the specification for which filter to apply.
   * @return a source for data in the Fiji table, whose row tuples will contain fields with cell
   *     data from the requested columns and map-type column families.
   */
  private[express] def apply(
      tableUri: String,
      timeRange: TimeRangeSpec,
      columns: Map[_ <: ColumnInputSpec, Symbol],
      rowRangeSpec: RowRangeSpec,
      rowFilterSpec: RowFilterSpec
  ): FijiSource = {
    new FijiSource(
        tableUri,
        timeRange,
        None,
        inputColumns = columns.map { entry: (ColumnInputSpec, Symbol) => entry.swap },
        rowRangeSpec = rowRangeSpec,
        rowFilterSpec = rowFilterSpec
    )
  }

  /**
   * Method for creating a TypedFijiSource.
   *
   * @param tableUri addressing a table in a Fiji instance.
   * @param timeRange that cells must fall into to be retrieved.
   * @param columns are a series of pairs mapping column input specs to tuple field names.
   *     Columns are specified as "family:qualifier" or, in the case of a column family input spec,
   *     simply "family".
   * @param rowRangeSpec the specification for which row interval to scan
   * @param rowFilterSpec the specification for which filter to apply.
   * @return a typed source for data in the Fiji table, whose row tuples will contain fields with
   *     cell data from the requested columns and map-type column families.
   */
  private[express] def typedFijiSource(
      tableUri: String,
      timeRange: TimeRangeSpec,
      columns: List[_ <: ColumnInputSpec],
      rowRangeSpec: RowRangeSpec,
      rowFilterSpec: RowFilterSpec
  ): TypedFijiSource[ExpressResult] = {
    new TypedFijiSource[ExpressResult](
        tableUri,
        timeRange,
        columns ,
        rowRangeSpec = rowRangeSpec,
        rowFilterSpec = rowFilterSpec
    )
  }
}
