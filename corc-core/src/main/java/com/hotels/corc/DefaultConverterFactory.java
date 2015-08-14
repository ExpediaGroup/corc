/**
 * Copyright 2015 Expedia Inc.
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
package com.hotels.corc;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObject;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class DefaultConverterFactory implements ConverterFactory {

  private static final long serialVersionUID = 1L;

  @Override
  public Converter newConverter(ObjectInspector inspector) {
    switch (inspector.getCategory()) {
    case PRIMITIVE:
      switch (((PrimitiveObjectInspector) inspector).getPrimitiveCategory()) {
      case STRING:
        return new StringConverter();
      case BOOLEAN:
        return new BooleanConverter();
      case BYTE:
        return new ByteConverter();
      case SHORT:
        return new ShortConverter();
      case INT:
        return new IntegerConverter();
      case LONG:
        return new LongConverter();
      case FLOAT:
        return new FloatConverter();
      case DOUBLE:
        return new DoubleConverter();
      case TIMESTAMP:
        return new TimestampConverter();
      case DATE:
        return new DateConverter();
      case BINARY:
        return new BinaryConverter();
      case CHAR:
        return new CharConverter();
      case VARCHAR:
        return new VarcharConverter();
      case DECIMAL:
        return new DecimalConverter();
      default:
        throw new IllegalArgumentException("Unknown Primitive Category: "
            + ((PrimitiveObjectInspector) inspector).getPrimitiveCategory());
      }
    case STRUCT:
      return new StructConverter(this, (SettableStructObjectInspector) inspector);
    case LIST:
      return new ListConverter(this, (ListObjectInspector) inspector);
    case MAP:
      return new MapConverter(this, (MapObjectInspector) inspector);
    case UNION:
      return new UnionConverter(this, (UnionObjectInspector) inspector);
    default:
      throw new IllegalArgumentException("Unknown Category: " + inspector.getCategory());
    }
  }

  public static class StringConverter extends BaseConverter {

    @Override
    protected Object toWritableObjectInternal(Object value) throws UnexpectedTypeException {
      return new Text((String) value);
    }

    @Override
    protected Object toJavaObjectInternal(Object value) throws UnexpectedTypeException {
      return ((Text) value).toString();
    }

  }

  public static class BooleanConverter extends BaseConverter {

    @Override
    protected Object toWritableObjectInternal(Object value) throws UnexpectedTypeException {
      return new BooleanWritable((Boolean) value);
    }

    @Override
    protected Object toJavaObjectInternal(Object value) throws UnexpectedTypeException {
      return ((BooleanWritable) value).get();
    }

  }

  public static class ByteConverter extends BaseConverter {

    @Override
    protected Object toWritableObjectInternal(Object value) throws UnexpectedTypeException {
      return new ByteWritable((Byte) value);
    }

    @Override
    protected Object toJavaObjectInternal(Object value) throws UnexpectedTypeException {
      return ((ByteWritable) value).get();
    }

  }

  public static class ShortConverter extends BaseConverter {

    @Override
    protected Object toWritableObjectInternal(Object value) throws UnexpectedTypeException {
      return new ShortWritable((Short) value);
    }

    @Override
    protected Object toJavaObjectInternal(Object value) throws UnexpectedTypeException {
      return ((ShortWritable) value).get();
    }

  }

  public static class IntegerConverter extends BaseConverter {

    @Override
    protected Object toWritableObjectInternal(Object value) throws UnexpectedTypeException {
      return new IntWritable((Integer) value);
    }

    @Override
    protected Object toJavaObjectInternal(Object value) throws UnexpectedTypeException {
      return ((IntWritable) value).get();
    }

  }

  public static class LongConverter extends BaseConverter {

    @Override
    protected Object toWritableObjectInternal(Object value) throws UnexpectedTypeException {
      return new LongWritable((Long) value);
    }

    @Override
    protected Object toJavaObjectInternal(Object value) throws UnexpectedTypeException {
      return ((LongWritable) value).get();
    }

  }

  public static class FloatConverter extends BaseConverter {

    @Override
    protected Object toWritableObjectInternal(Object value) throws UnexpectedTypeException {
      return new FloatWritable((Float) value);
    }

    @Override
    protected Object toJavaObjectInternal(Object value) throws UnexpectedTypeException {
      return ((FloatWritable) value).get();
    }

  }

  public static class DoubleConverter extends BaseConverter {

    @Override
    protected Object toWritableObjectInternal(Object value) throws UnexpectedTypeException {
      return new DoubleWritable((Double) value);
    }

    @Override
    protected Object toJavaObjectInternal(Object value) throws UnexpectedTypeException {
      return ((DoubleWritable) value).get();
    }

  }

  public static class TimestampConverter extends BaseConverter {

    @Override
    protected Object toWritableObjectInternal(Object value) throws UnexpectedTypeException {
      return new TimestampWritable((Timestamp) value);
    }

    @Override
    protected Object toJavaObjectInternal(Object value) throws UnexpectedTypeException {
      return ((TimestampWritable) value).getTimestamp();
    }

  }

  public static class DateConverter extends BaseConverter {

    @Override
    protected Object toWritableObjectInternal(Object value) throws UnexpectedTypeException {
      return new DateWritable((Date) value);
    }

    @Override
    protected Object toJavaObjectInternal(Object value) throws UnexpectedTypeException {
      // DateWritable applies timezone offset on get(), we don't want that
      int days = ((DateWritable) value).getDays();
      return new Date(TimeUnit.DAYS.toMillis(days));
    }

  }

  public static class BinaryConverter extends BaseConverter {

    @Override
    protected Object toWritableObjectInternal(Object value) throws UnexpectedTypeException {
      return new BytesWritable((byte[]) value);
    }

    @Override
    protected Object toJavaObjectInternal(Object value) throws UnexpectedTypeException {
      return ((BytesWritable) value).getBytes();
    }

  }

  public static class CharConverter extends BaseConverter {

    @Override
    protected Object toWritableObjectInternal(Object value) throws UnexpectedTypeException {
      return new HiveCharWritable((HiveChar) value);
    }

    @Override
    protected Object toJavaObjectInternal(Object value) throws UnexpectedTypeException {
      return ((HiveCharWritable) value).getHiveChar();
    }

  }

  public static class VarcharConverter extends BaseConverter {

    @Override
    protected Object toWritableObjectInternal(Object value) throws UnexpectedTypeException {
      return new HiveVarcharWritable((HiveVarchar) value);
    }

    @Override
    protected Object toJavaObjectInternal(Object value) throws UnexpectedTypeException {
      return ((HiveVarcharWritable) value).getHiveVarchar();
    }

  }

  public static class DecimalConverter extends BaseConverter {

    @Override
    protected Object toWritableObjectInternal(Object value) throws UnexpectedTypeException {
      return new HiveDecimalWritable((HiveDecimal) value);
    }

    @Override
    protected Object toJavaObjectInternal(Object value) throws UnexpectedTypeException {
      return ((HiveDecimalWritable) value).getHiveDecimal();
    }

  }

  public static class StructConverter extends BaseConverter {

    private final SettableStructObjectInspector inspector;
    private final List<Converter> converters = new ArrayList<>();

    public StructConverter(ConverterFactory factory, SettableStructObjectInspector inspector) {
      this.inspector = inspector;
      for (StructField structField : inspector.getAllStructFieldRefs()) {
        converters.add(factory.newConverter(structField.getFieldObjectInspector()));
      }
    }

    @Override
    protected Object toWritableObjectInternal(Object value) throws UnexpectedTypeException {
      @SuppressWarnings("unchecked")
      List<Object> list = (List<Object>) value;
      OrcStruct result = (OrcStruct) inspector.create();
      result.setNumFields(list.size());
      int i = 0;
      for (StructField field : inspector.getAllStructFieldRefs()) {
        inspector.setStructFieldData(result, field, converters.get(i).toWritableObject(list.get(i)));
        i++;
      }
      return result;
    }

    @Override
    protected Object toJavaObjectInternal(Object value) throws UnexpectedTypeException {
      OrcStruct struct = (OrcStruct) value;
      List<Object> result = new ArrayList<>(struct.getNumFields());
      int i = 0;
      for (StructField field : inspector.getAllStructFieldRefs()) {
        result.add(converters.get(i).toJavaObject(inspector.getStructFieldData(struct, field)));
        i++;
      }
      return result;
    }

  }

  public static class ListConverter extends BaseConverter {

    private final Converter converter;

    public ListConverter(ConverterFactory factory, ListObjectInspector inspector) {
      converter = factory.newConverter(inspector.getListElementObjectInspector());
    }

    @Override
    protected Object toWritableObjectInternal(Object value) throws UnexpectedTypeException {
      @SuppressWarnings("unchecked")
      List<Object> list = (List<Object>) value;
      List<Object> result = new ArrayList<>(list.size());
      for (Object item : list) {
        result.add(converter.toWritableObject(item));
      }
      return result;
    }

    @Override
    protected Object toJavaObjectInternal(Object value) throws UnexpectedTypeException {
      @SuppressWarnings("unchecked")
      List<Object> list = (List<Object>) value;
      List<Object> result = new ArrayList<>(list.size());
      for (Object item : list) {
        result.add(converter.toJavaObject(item));
      }
      return result;
    }

  }

  public static class MapConverter extends BaseConverter {

    private final Converter keyConverter;
    private final Converter valueConverter;

    public MapConverter(ConverterFactory factory, MapObjectInspector inspector) {
      keyConverter = factory.newConverter(inspector.getMapKeyObjectInspector());
      valueConverter = factory.newConverter(inspector.getMapValueObjectInspector());
    }

    @Override
    protected Object toWritableObjectInternal(Object value) throws UnexpectedTypeException {
      @SuppressWarnings("unchecked")
      Map<Object, Object> map = (Map<Object, Object>) value;
      Map<Object, Object> result = new HashMap<>(map.size());
      for (Entry<Object, Object> entry : map.entrySet()) {
        result.put(keyConverter.toWritableObject(entry.getKey()), valueConverter.toWritableObject(entry.getValue()));
      }
      return result;
    }

    @Override
    protected Object toJavaObjectInternal(Object value) throws UnexpectedTypeException {
      @SuppressWarnings("unchecked")
      Map<Object, Object> map = (Map<Object, Object>) value;
      Map<Object, Object> result = new HashMap<>(map.size());
      for (Entry<Object, Object> entry : map.entrySet()) {
        result.put(keyConverter.toJavaObject(entry.getKey()), valueConverter.toJavaObject(entry.getValue()));
      }
      return result;
    }

  }

  public static class UnionConverter extends BaseConverter {

    private final List<Converter> converters = new ArrayList<>();

    public UnionConverter(ConverterFactory factory, UnionObjectInspector inspector) {
      for (ObjectInspector child : inspector.getObjectInspectors()) {
        converters.add(factory.newConverter(child));
      }
    }

    @Override
    protected Object toWritableObjectInternal(Object value) throws UnexpectedTypeException {
      for (byte i = 0; i < converters.size(); i++) {
        try {
          Object writable = converters.get(i).toWritableObject(value);
          return OrcUnionFactory.newInstance(i, writable);
        } catch (UnexpectedTypeException e) {
          // try the next one
          continue;
        }
      }
      throw new UnexpectedTypeException(value);
    }

    @Override
    protected Object toJavaObjectInternal(Object value) throws UnexpectedTypeException {
      UnionObject union = (UnionObject) value;
      byte tag = union.getTag();
      Converter converter = converters.get(tag);
      return converter.toJavaObject(union.getObject());
    }

  }

}
