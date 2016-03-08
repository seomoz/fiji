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

package com.moz.fiji.hive.utils;

import static com.moz.fiji.hive.utils.HiveTypes.HiveList;
import static com.moz.fiji.hive.utils.HiveTypes.HiveMap;
import static com.moz.fiji.hive.utils.HiveTypes.HiveStruct;
import static com.moz.fiji.hive.utils.HiveTypes.HiveUnion;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts an Avro data object to an in-memory representation for Hive.
 *
 * <p>This prepares hive objects under the assumption that we are
 * using the "standard" object inspectors. See the classes in
 * org.apache.hadoop.hive.serde2.objectinspector that start with
 * "Standard" for details about how each Hive type should be formatted.</p>
 */
public final class AvroTypeAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(AvroTypeAdapter.class);

  /** Private constructor to prevent instantiation. Use get() to get an instance of this. */
  private AvroTypeAdapter() {}

  /** Singleton instance. */
  private static final AvroTypeAdapter SINGLETON = new AvroTypeAdapter();

  /** @return an instance of AvroTypeAdapter. */
  public static AvroTypeAdapter get() {
    return SINGLETON;
  }

  /**
   * Indicates that an Avro data type is not compatible with a Hive type.
   */
  public static class IncompatibleTypeException extends RuntimeException {
    /**
     * Constructor.
     *
     * @param hiveType The hive type.
     * @param avroData The avro data.
     */
    public IncompatibleTypeException(TypeInfo hiveType, Object avroData) {
      super("Unable to convert avro data [" + avroData + "] to hive type [" + hiveType + "]");
    }

    /**
     * Constructor.
     *
     * @param hiveType The hive type.
     */
    public IncompatibleTypeException(TypeInfo hiveType) {
      super("Unable to generate an avro schema that describes hive type [" + hiveType + "]");
    }
  }

  /**
   * Converts a piece avro data to a hive in-memory object.  This method will recursively
   * unpack the objects for any non-primitive types.
   *
   * @param hiveType The type of the target hive object.
   * @param avro The avro data to convert.
   * @param schema The schema the passed in type.
   * @return The converted hive datum, compatible with the standard object inspector.
   */
  public Object toHiveType(TypeInfo hiveType, Object avro, Schema schema) {
    if (null == avro) {
      return null;
    }
    Preconditions.checkNotNull(schema, "Schema must not be null.");

    switch (hiveType.getCategory()) {
    case PRIMITIVE:
      return toHiveType((PrimitiveTypeInfo) hiveType, avro);
    case LIST:
      HiveList<Object> hiveList = new HiveList<Object>();
      final List<Object> avroList = (List<Object>) avro;
      final TypeInfo listElementType = ((ListTypeInfo) hiveType).getListElementTypeInfo();
      for (Object avroElement : avroList) {
        hiveList.add(toHiveType(listElementType, avroElement, schema.getElementType()));
      }
      return hiveList;
    case MAP:
      HiveMap<String, Object> hiveMap = new HiveMap<String, Object>();
      @SuppressWarnings("unchecked")
      final Map<CharSequence, Object> avroMap = (Map<CharSequence, Object>) avro;
      final TypeInfo mapValueType = ((MapTypeInfo) hiveType).getMapValueTypeInfo();
      for (Map.Entry<CharSequence, Object> avroEntry : avroMap.entrySet()) {
        String entryKey = avroEntry.getKey().toString();
        Object entryValue = toHiveType(mapValueType, avroEntry.getValue(), schema.getValueType());
        hiveMap.put(entryKey, entryValue);
      }
      return hiveMap;
    case STRUCT:
      HiveStruct hiveStruct = new HiveStruct();
      final GenericRecord avroRecord = (GenericRecord) avro;
      final StructTypeInfo hiveStructType = (StructTypeInfo) hiveType;
      List<Schema> schemaList = Lists.newArrayList();
      for (Schema.Field field : schema.getFields()) {
        schemaList.add(field.schema());
      }
      for (int i = 0; i < hiveStructType.getAllStructFieldNames().size(); i++) {
        final String fieldName = hiveStructType.getAllStructFieldNames().get(i);
        final TypeInfo fieldType = hiveStructType.getAllStructFieldTypeInfos().get(i);
        hiveStruct.add(toHiveType(fieldType, avroRecord.get(fieldName), schemaList.get(i)));
      }
      return hiveStruct;
    case UNION:
      HiveUnion hiveUnion = new HiveUnion();
      final Integer tag = GenericData.get().resolveUnion(schema, avro);
      hiveUnion.setTag(tag.byteValue());
      Schema unionSubSchema = schema.getTypes().get(tag);

      final UnionTypeInfo hiveUnionType = (UnionTypeInfo) hiveType;
      final TypeInfo unionSubType = hiveUnionType.getAllUnionObjectTypeInfos().get(tag);
      hiveUnion.setObject(toHiveType(unionSubType, avro, unionSubSchema));
      return hiveUnion;
    default:
      throw new IncompatibleTypeException(hiveType, avro);
    }
  }

  /**
   * Converts data from Avro into a Hive primitive type.
   *
   * @param primitiveType The target Hive type.
   * @param avro The avro datum.
   * @return The converted Hive object.
   */
  public Object toHiveType(PrimitiveTypeInfo primitiveType, Object avro) {
    switch (primitiveType.getPrimitiveCategory()) {
    case VOID: // Like the avro null type, right?
      return null;

    case BYTE:
      if (!(avro instanceof GenericFixed)) {
        throw new IncompatibleTypeException(primitiveType, avro);
      }
      return Byte.valueOf(((GenericFixed) avro).bytes()[0]);

    case SHORT:
      if (!(avro instanceof GenericFixed)) {
        throw new IncompatibleTypeException(primitiveType, avro);
      }
      final byte[] fixedBytes = ((GenericFixed) avro).bytes();
      final ByteBuffer bb = ByteBuffer.allocate(2);
      bb.order(ByteOrder.LITTLE_ENDIAN);
      bb.put(fixedBytes[0]);
      bb.put(fixedBytes[1]);
      return bb.getShort(0);

    case BOOLEAN: // These primitive types are the same in avro and hive.
    case INT:
    case LONG:
    case FLOAT:
    case DOUBLE:
      return avro;

    case STRING:
      return avro.toString();

    case TIMESTAMP:
      if (!(avro instanceof Long)) {
        throw new IncompatibleTypeException(primitiveType, avro);
      }
      return new Timestamp(((Long) avro).longValue());

    case BINARY:
      final ByteArrayRef byteArrayRef = new ByteArrayRef();
      final ByteBuffer byteBuffer = (ByteBuffer) avro;
      byteArrayRef.setData(
          Arrays.copyOfRange(byteBuffer.array(), byteBuffer.position(), byteBuffer.limit()));
      return byteArrayRef;

    default:
      throw new IncompatibleTypeException(primitiveType, avro);
    }
  }

  /**
   * Converts a ObjectInspector from Hive to the appropriate Avro schema.
   *
   * @param objectInspector Hive ObjectInspector to convert.
   * @return Avro schema inferred from the PrimitiveObjectInspector.
   */
  public Schema toAvroSchema(ObjectInspector objectInspector) {
    switch (objectInspector.getCategory()) {
    case PRIMITIVE:
      return toAvroSchema((PrimitiveObjectInspector) objectInspector);
    case LIST:
      ListObjectInspector listObjectInspector = (ListObjectInspector) objectInspector;
      return Schema.createArray(
          toAvroSchema(listObjectInspector.getListElementObjectInspector()));
    case MAP:
      MapObjectInspector mapObjectInspector = (MapObjectInspector) objectInspector;
      return Schema.createMap(toAvroSchema(mapObjectInspector.getMapValueObjectInspector()));
    case STRUCT:
      List structFields = ((StructObjectInspector) objectInspector).getAllStructFieldRefs();
      List<Schema.Field> fields = Lists.newArrayList();
      for (Object structFieldObj : structFields) {
        StructField structField = (StructField) structFieldObj;
        String fieldName = structField.getFieldName();
        String fieldComment = structField.getFieldComment();
        Schema.Field field = new Schema.Field(fieldName,
            toAvroSchema(structField.getFieldObjectInspector()),
            fieldComment, null);
        fields.add(field);
      }
      return Schema.createRecord(fields);
    case UNION:
      UnionObjectInspector unionObjectInspector = (UnionObjectInspector) objectInspector;
      List<ObjectInspector> unionObjectInspectors = unionObjectInspector.getObjectInspectors();
      List<Schema> unionTypes = Lists.newArrayList();
      for (ObjectInspector unionSubObjectInspector : unionObjectInspectors) {
        unionTypes.add(toAvroSchema(unionSubObjectInspector));
      }
      return Schema.createUnion(unionTypes);
    default:
      throw new UnsupportedOperationException("Unknown type: " + objectInspector);
    }
  }

  /**
   * Converts a PrimitiveObjectInspector from Hive to the appropriate Avro schema.
   *
   * @param primitiveObjectInspector Hive PrimitiveObjectInspector to convert.
   * @return Avro schema inferred from the PrimitiveObjectInspector.
   */
  public Schema toAvroSchema(PrimitiveObjectInspector primitiveObjectInspector) {
    switch (primitiveObjectInspector.getPrimitiveCategory()) {
    case VOID: // Like the hiveObject null type, right?
      return Schema.create(Schema.Type.NULL);
    case BYTE:
      return Schema.createFixed("BYTE", "", "", 1);
    case SHORT:
      return Schema.createFixed("SHORT", "", "", 2);
    case BOOLEAN:
      return Schema.create(Schema.Type.BOOLEAN);
    case INT:
      return Schema.create(Schema.Type.INT);
    case LONG:
      return Schema.create(Schema.Type.LONG);
    case FLOAT:
      return Schema.create(Schema.Type.FLOAT);
    case DOUBLE:
      return Schema.create(Schema.Type.DOUBLE);
    case STRING:
      return Schema.create(Schema.Type.STRING);
    case TIMESTAMP:
      return Schema.create(Schema.Type.LONG);
    case BINARY:
      return Schema.create(Schema.Type.BYTES);
    default:
      throw new UnsupportedOperationException("Unknown type: " + primitiveObjectInspector);
    }
  }
}
