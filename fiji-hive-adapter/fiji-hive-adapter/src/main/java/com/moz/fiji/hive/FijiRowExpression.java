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

package com.moz.fiji.hive;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.hive.io.EntityIdWritable;
import com.moz.fiji.hive.io.FijiCellWritable;
import com.moz.fiji.hive.io.FijiRowDataWritable;
import com.moz.fiji.hive.utils.AvroTypeAdapter;
import com.moz.fiji.hive.utils.AvroTypeAdapter.IncompatibleTypeException;
import com.moz.fiji.hive.utils.HiveTypes.HiveList;
import com.moz.fiji.hive.utils.HiveTypes.HiveMap;
import com.moz.fiji.hive.utils.HiveTypes.HiveStruct;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequestBuilder;

/**
 * A FijiRowExpression is a string that addresses a piece of data inside a FijiTable row.
 *
 * <p>Data can be addressed by specifying an entire family, a specific
 * column, or even a particular field within an cell. This may someday
 * be extended to allow wildcards, for example to create arrays of
 * fields inside records across multiple cells.</p>
 *
 * <p>Valid expressions:</p>
 * <ul>
 *  <li> family - map[string, array[struct[int, fields...]]]</li>
 *  <li> family[0] - map[string, struct[int, fields...]]</li>
 *  <li> family:qualifier - array[struct[int, fields...]]</li>
 *  <li> family:qualifier[3] - struct[int, fields...]</li>
 *  <li> family:qualifier[0].field - fieldtype</li>
 *  <li> family:qualifier[-1].timestamp - timestamp (of the oldest cell)</li>
 * </ul>
 */
public class FijiRowExpression {
  private static final Logger LOG = LoggerFactory.getLogger(FijiRowExpression.class);

  /** The parsed expression. */
  private final Expression mExpression;

  /**
   * Creates an expression.
   *
   * @param expression The expression string.
   * @param typeInfo The Hive type the expression is mapped to.
   */
  public FijiRowExpression(String expression, TypeInfo typeInfo) {
    mExpression = new Parser().parse(StringUtils.trim(expression), typeInfo);
  }

  /**
   * Evaluates an expression in the context of a Fiji row.
   *
   * @param row A fiji row.
   * @return The data addressed by the expression.
   * @throws IOException If there is an IO error.
   */
  public Object evaluate(FijiRowDataWritable row) throws IOException {
    return new Evaluator().evaluate(mExpression, row);
  }

  /**
   * Gets the data request required to evaluate this expression.
   *
   * @return The data request.
   */
  public FijiDataRequest getDataRequest() {
    return mExpression.getDataRequest();
  }

  /**
   * Converts a Hive object(with associated ObjectInspector) to a time series data that is more
   * representative of a Fiji internal representation.
   *
   *
   * @param objectInspector that defines the Hive format of the object.
   * @param hiveObject containing the data that is to be converted into a time series format.
   * @return timeseries data suitable for writing into Fiji.
   */
  public Map<FijiColumnName, NavigableMap<Long, FijiCellWritable>> convertToTimeSeries(
      ObjectInspector objectInspector, Object hiveObject) {
    return mExpression.convertToTimeSeries(objectInspector, hiveObject);
  }

  /**
   * Determines whether this expression is mapped to FijiCell(s) or not(rowkey information would
   * result in false for example).
   *
   * @return whether this expression is mapped to FijiCell(s).
   */
  public boolean isCellData() {
    return mExpression instanceof ValueExpression;
  }

  /**
   * A parsed expression.
   *
   * <p>Row expression map into the implementations of this interface</p>
   * <li>family - {@link FamilyAllValuesExpression}
   * <li>family[n] - {@link FamilyFlatValueExpression}
   * <li>family:qualifier - {@link ColumnAllValuesExpression}
   * <li>family:qualifier[n] - {@link ColumnFlatValueExpression}
   */
  private interface Expression {
    /**
     * Determines whether the expression represents a value (not an operator).
     *
     * @return Whether this expression is a value.
     */
    boolean isValue();

    /**
     * Gets the value in the context of a row.
     *
     * @param row A fiji row.
     * @return The value.
     * @throws IOException If there is an IO error.
     */
    Object getValue(FijiRowDataWritable row) throws IOException;

    /**
     * Gets the operands of this operator.
     *
     * @return The operands.
     * @throws UnsupportedOperationException If this is not an operator.
     */
    List<Expression> getOperands();

    /**
     * Evaluates the operator expression given the operands.
     *
     * @param operandValues The values of the operands.
     * @return The result.
     */
    Object eval(List<Object> operandValues);

    /**
     * Converts Hive object into timeseries data using the associated ObjectInspector.
     *
     * @param objectInspector that defines the format of the object passed in from Hive.
     * @param hiveObject that contains the data to be translated into a timeseries.
     * @return Timeseries data
     */
    Map<FijiColumnName, NavigableMap<Long, FijiCellWritable>> convertToTimeSeries(
        ObjectInspector objectInspector, Object hiveObject);

    /**
     * Gets the data request required to evaluate this expression.
     *
     * @return The data request.
     */
    FijiDataRequest getDataRequest();
  }

  /**
   * An expression that represents a data value on its own (not an operator).
   */
  private abstract static class ValueExpression implements Expression {
    /** The Hive type of the value. */
    private final TypeInfo mTypeInfo;

    /** The FijiColumnName. */
    private final FijiColumnName mFijiColumnName;

    /**
     * Constructor.
     *
     * @param typeInfo The Hive type.
     * @param fijiColumnName The Fiji column for this expression.
     */
    protected ValueExpression(TypeInfo typeInfo, FijiColumnName fijiColumnName) {
      mTypeInfo = typeInfo;
      mFijiColumnName = fijiColumnName;
    }

    /**
     * Gets the Hive type.
     *
     * @return The Hive type.
     */
    protected TypeInfo getTypeInfo() {
      return mTypeInfo;
    }

    /**
     * Gets an Avro type adapter for converting Avro and Hive data/types.
     *
     * @return The adapter.
     */
    protected AvroTypeAdapter getAvroTypeAdapter() {
      return AvroTypeAdapter.get();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isValue() {
      return true;
    }

    /** {@inheritDoc} */
    @Override
    public List<Expression> getOperands() {
      throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public Object eval(List<Object> operandValues) {
      throw new UnsupportedOperationException();
    }

    /**
     * Gets the Fiji column family name.
     *
     * @return The Fiji column family name.
     */
    protected String getFamily() {
      return mFijiColumnName.getFamily();
    }

    /**
     * Gets the Fiji column qualifier name.
     *
     * @return The Fiji column qualifier name.
     */
    protected String getQualifier() {
      return mFijiColumnName.getQualifier();
    }

    /**
     * Gets the full FijiColumnName.
     *
     * @return The full FijiColumnName.
     */
    protected FijiColumnName getFijiColumnName() {
      return mFijiColumnName;
    }
  }

  /**
   * An expression that reads the EntityId as a string.
   *
   * Returns results with the Hive type: STRING.
   */
  private static class EntityIdExpression implements Expression {
    /** The Hive type of the value. */
    private final TypeInfo mTypeInfo;

    /**
     * Constructor.
     *
     * @param typeInfo The Hive type.
     */
    public EntityIdExpression(TypeInfo typeInfo) {
      mTypeInfo = typeInfo;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isValue() {
      return true;
    }

    /** {@inheritDoc} */
    @Override
    public Object getValue(FijiRowDataWritable row) throws IOException {
      final EntityIdWritable entityId = row.getEntityId();
      return entityId.toShellString();
    }

    /** {@inheritDoc} */
    @Override
    public FijiDataRequest getDataRequest() {
      final FijiDataRequestBuilder builder = FijiDataRequest.builder();
      return builder.build();
    }

    /** {@inheritDoc} */
    @Override
    public List<Expression> getOperands() {
      throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public Object eval(List<Object> operandValues) {
      throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public Map<FijiColumnName, NavigableMap<Long, FijiCellWritable>> convertToTimeSeries(
        ObjectInspector objectInspector, Object hiveObject) {
      throw new UnsupportedOperationException("EntityId expressions do not map to a timeseries.");
    }
  }

  /**
   * An expression that reads a component of the EntityId.
   *
   * Returns results with the Hive type: one of INT, BIGINT, STRING.
   */
  private static class EntityIdComponentExpression implements Expression {
    /** The Hive type of the value. */
    private final PrimitiveTypeInfo mTypeInfo;

    /** The index of the component to read from the EntityId. */
    private final int mIndex;

    /**
     * Constructor.
     *
     * @param typeInfo The Hive type.
     * @param index The index of the component in the EntityId.
     */
    public EntityIdComponentExpression(TypeInfo typeInfo, int index) {
      if (!(typeInfo instanceof PrimitiveTypeInfo)) {
        throw new IllegalArgumentException(
            "Illegal type [" + typeInfo + "] for EntityId component. "
            + "Must be one of INT, BIGINT, or STRING.");
      }

      if (index < 0) {
        throw new IllegalArgumentException(
            "Illegal index [" + index + "] for EntityId component");
      }

      mTypeInfo = (PrimitiveTypeInfo) typeInfo;
      mIndex = index;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isValue() {
      return true;
    }

    /** {@inheritDoc} */
    @Override
    public Object getValue(FijiRowDataWritable row) throws IOException {
      final EntityIdWritable entityId = row.getEntityId();
      Object component = entityId.getComponents().get(mIndex);

      switch (mTypeInfo.getPrimitiveCategory()) {

      case INT:
      case LONG:
        return component;

      case STRING:
        return component.toString();

      default:
        throw new IncompatibleTypeException(mTypeInfo, component);
      }
    }

    /** {@inheritDoc} */
    @Override
    public FijiDataRequest getDataRequest() {
      final FijiDataRequestBuilder builder = FijiDataRequest.builder();
      return builder.build();
    }

    /** {@inheritDoc} */
    @Override
    public List<Expression> getOperands() {
      throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public Object eval(List<Object> operandValues) {
      throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public Map<FijiColumnName, NavigableMap<Long, FijiCellWritable>> convertToTimeSeries(
        ObjectInspector objectInspector, Object hiveObject) {
      throw new UnsupportedOperationException("EntityId expressions do not map to a timeseries.");
    }
  }

  /**
   * An expression that reads a particular cell index from a map type Fiji column family.
   *
   * Returns results with the Hive type: MAP<STRING, STRUCT<TIMESTAMP, cell>>
   */
  private static class FamilyFlatValueExpression extends ValueExpression {
    /** Declared (therefore, the target) Hive type for the cell data. */
    private final TypeInfo mCellTypeInfo;

    /** The index of the cell to read from the column family (newest is zero). */
    private final int mIndex;

    /**
     * Constructor.
     *
     * @param typeInfo The Hive type.
     * @param familyName The Fiji column family to read from.
     * @param index The index of the cell to read from the family (newest is zero).
     */
    public FamilyFlatValueExpression(TypeInfo typeInfo, FijiColumnName familyName, int index) {
      super(typeInfo, familyName);

      // Gets the declared (therefore, the target) Hive type for the cell data.
      final MapTypeInfo mapTypeInfo = (MapTypeInfo) getTypeInfo();
      final StructTypeInfo structTypeInfo = (StructTypeInfo) mapTypeInfo.getMapValueTypeInfo();
      mCellTypeInfo = structTypeInfo.getAllStructFieldTypeInfos().get(1);

      // Gets the flattened index to retrieve(or -1 for the oldest)
      if (index < -1) {
        throw new IllegalArgumentException(
            "Illegal index [" + index + "] for family expression " + familyName);
      }
      mIndex = index;
    }

    /** {@inheritDoc} */
    @Override
    public Object getValue(FijiRowDataWritable row) throws IOException {
      final HiveMap<String, HiveStruct> result = new HiveMap<String, HiveStruct>();
      if (!row.containsColumn(getFamily())) {
        // TODO: Consider logging LOG.warn("Nothing found for {}", getFamily());
        return result;
      }

      final NavigableMap<String, NavigableMap<Long, Object>> familyMap =
          row.getValues(getFamily());
      for (Map.Entry<String, NavigableMap<Long, Object>> entry : familyMap.entrySet()) {
        Map.Entry<Long, Object> cell;
        if (-1 == mIndex) {
          cell = entry.getValue().lastEntry();
        } else {
          final Iterator<Map.Entry<Long, Object>> cellIterator =
              entry.getValue().entrySet().iterator();
          for (int i = 0; i < mIndex && cellIterator.hasNext(); i++) {
            cellIterator.next();
          }
          if (!cellIterator.hasNext()) {
            continue;
          }
          cell = cellIterator.next();
        }
        if (null != cell) {
          final HiveStruct struct = new HiveStruct();
          // Add the cell timestamp.
          struct.add(new Timestamp(cell.getKey()));
          // Add the cell value.
          struct.add(getAvroTypeAdapter().toHiveType(
              mCellTypeInfo,
              cell.getValue(),
              row.getReaderSchema(getFamily(), entry.getKey())));
          result.put(entry.getKey(), struct);
        }
      }
      return result;
    }

    /** {@inheritDoc} */
    @Override
    public FijiDataRequest getDataRequest() {
      // Indexes start from 0, whereas maxVersions starts from 1 so we need to adjust for this
      int maxVersions = mIndex + 1;
      if (mIndex == -1) {
        // If we are getting the oldest cell, we need all versions.
        maxVersions = HConstants.ALL_VERSIONS;
      }

      FijiDataRequestBuilder builder = FijiDataRequest.builder();
      builder.newColumnsDef().withMaxVersions(maxVersions).addFamily(getFamily());
      return builder.build();
    }

    /** {@inheritDoc} */
    @Override
    public Map<FijiColumnName, NavigableMap<Long, FijiCellWritable>> convertToTimeSeries(
        ObjectInspector objectInspector, Object hiveObject) {
      Map<FijiColumnName, NavigableMap<Long, FijiCellWritable>> expressionData = Maps.newHashMap();


      MapObjectInspector mapObjectInspector = (MapObjectInspector) objectInspector;
      Map mapData = mapObjectInspector.getMap(hiveObject);
      for (Object key : mapData.keySet()) {
        NavigableMap<Long, FijiCellWritable> timeseries = Maps.newTreeMap();

        // Assumes that this key is a string.
        Preconditions.checkState(key instanceof String, "Hive Map key must be a string");
        String qualifier = (String) key;
        StructObjectInspector structObjectInspector =
            (StructObjectInspector) mapObjectInspector.getMapValueObjectInspector();

        Object mapValueObject = mapObjectInspector.getMapValueElement(hiveObject, key);
        FijiCellWritable fijiCellWritable =
            new FijiCellWritable(structObjectInspector, mapValueObject);
        if (fijiCellWritable.hasData()) {
          timeseries.put(fijiCellWritable.getTimestamp(), fijiCellWritable);
        }
        FijiColumnName fijiColumnName = new FijiColumnName(getFamily(), qualifier);
        expressionData.put(fijiColumnName, timeseries);
      }

      return expressionData;
    }
  }

  /**
   * An expression that reads all cells from from a map type Fiji column family.
   *
   * Returns results with the Hive type: MAP<STRING, ARRAY<STRUCT<TIMESTAMP, cell>>>
   */
  private static class FamilyAllValuesExpression extends ValueExpression {
    /** Declared (therefore, the target) Hive type for the cell data. */
    private final TypeInfo mCellTypeInfo;

    /**
     * Constructor.
     *
     * @param typeInfo The Hive type.
     * @param familyName The Wibi column family.
     */
    public FamilyAllValuesExpression(TypeInfo typeInfo, FijiColumnName familyName) {
      super(typeInfo, familyName);

      // Gets the declared (therefore, the target) Hive type for the Fiji cell data.
      final MapTypeInfo mapTypeInfo = (MapTypeInfo) getTypeInfo();
      final ListTypeInfo listTypeInfo = (ListTypeInfo) mapTypeInfo.getMapValueTypeInfo();
      final StructTypeInfo structTypeInfo = (StructTypeInfo) listTypeInfo.getListElementTypeInfo();
      mCellTypeInfo =  structTypeInfo.getAllStructFieldTypeInfos().get(1);
    }

    /** {@inheritDoc} */
    @Override
    public Object getValue(FijiRowDataWritable row) throws IOException {
      final HiveMap<String, HiveList<HiveStruct>> result =
          new HiveMap<String, HiveList<HiveStruct>>();
      if (!row.containsColumn(getFamily())) {
        // TODO: Consider logging LOG.warn("Nothing found for {}", getFamily());
        return result;
      }

      final NavigableMap<String, NavigableMap<Long, Object>> familyMap =
          row.getValues(getFamily());
      for (Map.Entry<String, NavigableMap<Long, Object>> entry : familyMap.entrySet()) {
        final HiveList<HiveStruct> timeseries = new HiveList<HiveStruct>();
        for (Map.Entry<Long, Object> cell : entry.getValue().entrySet()) {
          final HiveStruct struct = new HiveStruct();
          // Add the cell timestamp.
          struct.add(new Timestamp(cell.getKey()));
          // Add the cell value.
          struct.add(getAvroTypeAdapter().toHiveType(
              mCellTypeInfo,
              cell.getValue(),
              row.getReaderSchema(getFamily(), entry.getKey())));
          timeseries.add(struct);
        }
        result.put(entry.getKey(), timeseries);
      }

      return result;
    }

    /** {@inheritDoc} */
    @Override
    public FijiDataRequest getDataRequest() {
      FijiDataRequestBuilder builder = FijiDataRequest.builder();
      builder.newColumnsDef().withMaxVersions(HConstants.ALL_VERSIONS)
          .addFamily(getFamily());
      return builder.build();
    }

    @Override
    public Map<FijiColumnName, NavigableMap<Long, FijiCellWritable>> convertToTimeSeries(
        ObjectInspector objectInspector, Object hiveObject) {
      Map<FijiColumnName, NavigableMap<Long, FijiCellWritable>> expressionData = Maps.newHashMap();

      MapObjectInspector familyAllValuesOI = (MapObjectInspector) objectInspector;
      Map familyAllValuesMap = familyAllValuesOI.getMap(hiveObject);

      for (Object qualifierObject : familyAllValuesMap.keySet()) {
        String qualifier = (String) qualifierObject;

        ListObjectInspector allValuesOI =
             (ListObjectInspector) familyAllValuesOI.getMapValueObjectInspector();
        List<Object> allValuesObjects =
            (List<Object>) familyAllValuesOI.getMapValueElement(hiveObject, qualifierObject);
        StructObjectInspector timestampedCellOI =
            (StructObjectInspector) allValuesOI.getListElementObjectInspector();

        NavigableMap<Long, FijiCellWritable> timeseries = Maps.newTreeMap();
        for (Object obj : allValuesObjects) {
          FijiCellWritable fijiCellWritable = new FijiCellWritable(timestampedCellOI, obj);
          if (fijiCellWritable.hasData()) {
            timeseries.put(fijiCellWritable.getTimestamp(), fijiCellWritable);
          }
        }
        FijiColumnName fijiColumnName = new FijiColumnName(getFamily(), qualifier);
        expressionData.put(fijiColumnName, timeseries);
      }

      return expressionData;
    }
  }

  /**
   * An expression that reads a single cell value from a Fiji table column.
   *
   * Returns results with the Hive type: STRUCT<TIMESTAMP, cell>
   */
  private static class ColumnFlatValueExpression extends ValueExpression {
    /** The index of the cell to read from the Fiji table column (newest is zero). */
    private final int mIndex;

    /** Declared (therefore, the target) Hive type for the cell data. */
    private final TypeInfo mCellTypeInfo;

    /**
     * Constructor.
     *
     * @param typeInfo The Hive type.
     * @param fijiColumnName The Fiji Column name
     * @param index The index of the cell to read from the column (newest is zero).
     */
    public ColumnFlatValueExpression(
        TypeInfo typeInfo, FijiColumnName fijiColumnName, int index) {
      super(typeInfo, fijiColumnName);

      // Gets the declared (therefore, the target) Hive type for the Fiji cell data.
      final StructTypeInfo structTypeInfo = (StructTypeInfo) getTypeInfo();
      mCellTypeInfo = structTypeInfo.getAllStructFieldTypeInfos().get(1);

      // Gets the flattened index to retrieve(or -1 for the oldest)
      if (index < -1) {
        throw new IllegalArgumentException(
            "Illegal index [" + index + "] for column expression "
            + fijiColumnName.toString());
      }
      mIndex = index;
    }

    /** {@inheritDoc} */
    @Override
    public Object getValue(FijiRowDataWritable row) throws IOException {
      final HiveStruct result = new HiveStruct();
      // Validate that the row contains data for the specified expression, and return empty struct
      // if nothing is found
      if (!row.containsColumn(getFamily(), getQualifier())) {
        // TODO: Consider logging LOG.warn("Nothing found for {}:{}", getFamily(), getQualifier());
        return result;
      }

      final NavigableMap<Long, Object> cellMap =
          row.getValues(getFamily(), getQualifier());

      Map.Entry<Long, Object> cell;
      if (-1 == mIndex) {
        cell = cellMap.lastEntry();
      } else {
        final Iterator<Map.Entry<Long, Object>> cellIterator = cellMap.entrySet().iterator();
        for (int i = 0; i < mIndex && cellIterator.hasNext(); i++) {
          cell = cellIterator.next();
        }
        if (!cellIterator.hasNext()) {
          return null;
        }
        cell = cellIterator.next();
      }

      // Add the cell timestamp.
      result.add(new Timestamp(cell.getKey()));
      // Add the cell value.
      result.add(getAvroTypeAdapter().toHiveType(
          mCellTypeInfo,
          cell.getValue(),
          row.getReaderSchema(getFamily(), getQualifier())));
      return result;
    }

    /** {@inheritDoc} */
    @Override
    public FijiDataRequest getDataRequest() {
      // Indexes start from 0, whereas maxVersions starts from 1 so we need to adjust for this
      int maxVersions = mIndex + 1;
      if (mIndex == -1) {
        // If we are getting the oldest cell, we need all versions.
        maxVersions = HConstants.ALL_VERSIONS;
      }

      FijiDataRequestBuilder builder = FijiDataRequest.builder();
      builder.newColumnsDef().withMaxVersions(maxVersions).add(getFamily(), getQualifier());
      return builder.build();
    }

    /** {@inheritDoc} */
    @Override
    public Map<FijiColumnName, NavigableMap<Long, FijiCellWritable>> convertToTimeSeries(
        ObjectInspector objectInspector, Object hiveObject) {
      Map<FijiColumnName, NavigableMap<Long, FijiCellWritable>> expressionData = Maps.newHashMap();

      NavigableMap<Long, FijiCellWritable> timeseries = Maps.newTreeMap();

      StructObjectInspector structObjectInspector = (StructObjectInspector) objectInspector;
      FijiCellWritable fijiCellWritable = new FijiCellWritable(structObjectInspector, hiveObject);
      if (fijiCellWritable.hasData()) {
        timeseries.put(fijiCellWritable.getTimestamp(), fijiCellWritable);
      }

      expressionData.put(getFijiColumnName(), timeseries);
      return expressionData;
    }
  }

  /**
   * An expression that reads all the cells from a Fiji table column.
   *
   * Returns results with the Hive type: ARRAY<STRUCT<TIMESTAMP, cell>>
   */
  private static class ColumnAllValuesExpression extends ValueExpression {
    /** Declared (therefore, the target) Hive type for the cell data. */
    private final TypeInfo mCellTypeInfo;

    /**
     * Constructor.
     *
     * @param typeInfo The Hive type.
     * @param fijiColumnName  The Fiji column name.
     */
    public ColumnAllValuesExpression(TypeInfo typeInfo, FijiColumnName fijiColumnName) {
      super(typeInfo, fijiColumnName);

      // Gets the declared (therefore, the target) Hive type for the Fiji cell value.
      final ListTypeInfo listTypeInfo = (ListTypeInfo) getTypeInfo();
      final StructTypeInfo structTypeInfo = (StructTypeInfo) listTypeInfo.getListElementTypeInfo();
      mCellTypeInfo =  structTypeInfo.getAllStructFieldTypeInfos().get(1);
    }

    /** {@inheritDoc} */
    @Override
    public Object getValue(FijiRowDataWritable row) throws IOException {
      final HiveList<HiveStruct> result = new HiveList<HiveStruct>();
      // Validate that the row contains data for the specified expression, and return empty struct
      // if nothing is found
      if (!row.containsColumn(getFamily(), getQualifier())) {
        // TODO: Consider logging LOG.warn("Nothing found for {}:{}", getFamily(), getQualifier());
        return result;
      }

      final NavigableMap<Long, Object> cellMap =
          row.getValues(getFamily(), getQualifier());
      for (Map.Entry<Long, Object> cell : cellMap.entrySet()) {
        final HiveStruct struct = new HiveStruct();
        // Add the cell timestamp.
        struct.add(new Timestamp(cell.getKey()));
        // Add the cell value.
        struct.add(getAvroTypeAdapter().toHiveType(
            mCellTypeInfo,
            cell.getValue(),
            row.getReaderSchema(getFamily(), getQualifier())));
        result.add(struct);
      }

      return result;
    }

    /** {@inheritDoc} */
    @Override
    public FijiDataRequest getDataRequest() {
      FijiDataRequestBuilder builder = FijiDataRequest.builder();
      builder.newColumnsDef()
          .withMaxVersions(HConstants.ALL_VERSIONS)
          .add(getFamily(), getQualifier());
      return builder.build();
    }

    /** {@inheritDoc} */
    @Override
    public Map<FijiColumnName, NavigableMap<Long, FijiCellWritable>> convertToTimeSeries(
        ObjectInspector objectInspector, Object hiveObject) {
      Map<FijiColumnName, NavigableMap<Long, FijiCellWritable>> expressionData = Maps.newHashMap();

      NavigableMap<Long, FijiCellWritable> timeseries = Maps.newTreeMap();

      ListObjectInspector listObjectInspector = (ListObjectInspector) objectInspector;
      List<Object> listObjects = (List<Object>) listObjectInspector.getList(hiveObject);
      StructObjectInspector structObjectInspector =
          (StructObjectInspector) listObjectInspector.getListElementObjectInspector();
      for (Object obj : listObjects) {
        FijiCellWritable fijiCellWritable = new FijiCellWritable(structObjectInspector, obj);
        if (fijiCellWritable.hasData()) {
          timeseries.put(fijiCellWritable.getTimestamp(), fijiCellWritable);
        }
      }

      expressionData.put(getFijiColumnName(), timeseries);
      return expressionData;
    }
  }

  /**
   * Turns a string expression into a structured tree that can be
   * evaluated given the data in a Fiji row.
   */
  private static class Parser {
    /** The regular expression for Fiji row expressions. */
    private static final String REGEX =
        // turn on comment and whitespace ignoring
        "(?x)"
        // match the literal '_entity_id' followed by an optional index within []
        + "(:entity_id(\\[(\\d+)\\])?)"
        + "|" // or
        // match the family name
        + "([A-Za-z0-9_]*)"
        // match zero or one qualifiers
        + "(:([^.\\[]*))?"
        // match zero or one column timestamp indexes
        + "(\\[(-?\\d+)\\])?"
        // match zero or more extensions(unused currently)
        + "([.]([a-z]+))*";

    /** The compiled pattern for Fiji row expressions. */
    private static final Pattern PATTERN = Pattern.compile(REGEX);

    private static final int ENTITY_ID_GROUP = 1;
    private static final int ENTITY_COMPONENT_GROUP = 2;
    private static final int ENTITY_COMPONENT_INDEX_GROUP = 3;
    private static final int FAMILY_GROUP = 4;
    private static final int QUALIFIER_DELIM_GROUP = 5;
    private static final int QUALIFIER_GROUP = 6;
    private static final int TS_GROUP = 7;
    private static final int TS_INDEX_GROUP = 8;

    /**
     * Parses a string expression.
     *
     * @param expression The expression string.
     * @param typeInfo The target Hive type the evaluated expression should be in.
     * @return The parsed expression.
     */
    public Expression parse(String expression, TypeInfo typeInfo) {
      // TODO: Use ANTLR or some other engine to allow for more
      // expressive expressions in the future.

      final Matcher matcher = PATTERN.matcher(expression);
      if (!matcher.matches()) {
        // TODO: Make a new type for this exception.
        throw new RuntimeException("Invalid fiji row expression: " + expression);
      }

      if (null != matcher.group(ENTITY_ID_GROUP)) {
        if (null == matcher.group(ENTITY_COMPONENT_GROUP)) {
          return new EntityIdExpression(typeInfo);
        }
        Integer index = Integer.valueOf(matcher.group(ENTITY_COMPONENT_INDEX_GROUP));
        return new EntityIdComponentExpression(typeInfo, index);
      }

      final String family = matcher.group(FAMILY_GROUP);
      if (null == matcher.group(QUALIFIER_DELIM_GROUP)) {
        FijiColumnName fijiFamilyName = new FijiColumnName(family);
        if (null == matcher.group(TS_GROUP)) {
          return new FamilyAllValuesExpression(typeInfo, fijiFamilyName);
        }
        Integer index = Integer.valueOf(matcher.group(TS_INDEX_GROUP));
        if (index < -1) {
          throw new RuntimeException("Invalid index(must be >= -1): " + index);
        }
        return new FamilyFlatValueExpression(typeInfo, fijiFamilyName, index);
      }

      final String qualifier = matcher.group(QUALIFIER_GROUP);
      FijiColumnName fijiColumnName = new FijiColumnName(family, qualifier);
      if (null == matcher.group(TS_GROUP)) {
        return new ColumnAllValuesExpression(typeInfo, fijiColumnName);
      }
      Integer index = Integer.valueOf(matcher.group(TS_INDEX_GROUP));
      if (index < -1) {
        throw new RuntimeException("Invalid index(must be >= -1): " + index);
      }
      return new ColumnFlatValueExpression(typeInfo, fijiColumnName, index);

      // TODO: Parse other operators on the values.
    }
  }

  /**
   * Evaluates a parsed Fiji row expression.
   */
  private static class Evaluator {
    /**
     * Evaluates a parsed expression in the context of a Fiji row.
     *
     * @param expression A parsed expression.
     * @param row A Fiji table row.
     * @return The evaluated expression data.
     * @throws IOException If there is an IO error reading from the Fiji row.
     */
    public Object evaluate(Expression expression, FijiRowDataWritable row) throws IOException {
      if (expression.isValue()) {
        return expression.getValue(row);
      }
      List<Object> operandValues = new ArrayList<Object>();
      for (Expression operand : expression.getOperands()) {
        operandValues.add(evaluate(operand, row));
      }
      return expression.eval(operandValues);
    }
  }
}
