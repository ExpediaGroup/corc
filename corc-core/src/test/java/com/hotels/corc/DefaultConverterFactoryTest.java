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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObject;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.hotels.corc.Converter;
import com.hotels.corc.ConverterFactory;
import com.hotels.corc.DefaultConverterFactory;
import com.hotels.corc.StructTypeInfoBuilder;
import com.hotels.corc.UnexpectedTypeException;

public class DefaultConverterFactoryTest {

  private final ConverterFactory factory = new DefaultConverterFactory();

  @Test
  public void stringJava() throws UnexpectedTypeException {
    Converter converter = factory.newConverter(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    assertThat(converter.toJavaObject(new Text("x")), is((Object) "x"));
  }

  @Test
  public void stringWritable() throws UnexpectedTypeException {
    Converter converter = factory.newConverter(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    assertThat(converter.toWritableObject("x"), is((Object) new Text("x")));
  }

  @Test
  public void stringNullJava() throws UnexpectedTypeException {
    Converter converter = factory.newConverter(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    assertThat(converter.toJavaObject(null), is(nullValue()));
  }

  @Test
  public void stringNullWritable() throws UnexpectedTypeException {
    Converter converter = factory.newConverter(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    assertThat(converter.toWritableObject(null), is(nullValue()));
  }

  @Test
  public void booleanJava() throws UnexpectedTypeException {
    Converter converter = factory.newConverter(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);
    assertThat(converter.toJavaObject(new BooleanWritable(true)), is((Object) true));
  }

  @Test
  public void booleanWritable() throws UnexpectedTypeException {
    Converter converter = factory.newConverter(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);
    assertThat(converter.toWritableObject(true), is((Object) new BooleanWritable(true)));
  }

  @Test
  public void byteJava() throws UnexpectedTypeException {
    Converter converter = factory.newConverter(PrimitiveObjectInspectorFactory.javaByteObjectInspector);
    assertThat(converter.toJavaObject(new ByteWritable((byte) 1)), is((Object) (byte) 1));
  }

  @Test
  public void byteWritable() throws UnexpectedTypeException {
    Converter converter = factory.newConverter(PrimitiveObjectInspectorFactory.javaByteObjectInspector);
    assertThat(converter.toWritableObject((byte) 1), is((Object) new ByteWritable((byte) 1)));
  }

  @Test
  public void shortJava() throws UnexpectedTypeException {
    Converter converter = factory.newConverter(PrimitiveObjectInspectorFactory.javaShortObjectInspector);
    assertThat(converter.toJavaObject(new ShortWritable((short) 1)), is((Object) (short) 1));
  }

  @Test
  public void shortWritable() throws UnexpectedTypeException {
    Converter converter = factory.newConverter(PrimitiveObjectInspectorFactory.javaShortObjectInspector);
    assertThat(converter.toWritableObject((short) 1), is((Object) new ShortWritable((short) 1)));
  }

  @Test
  public void intJava() throws UnexpectedTypeException {
    Converter converter = factory.newConverter(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
    assertThat(converter.toJavaObject(new IntWritable(1)), is((Object) 1));
  }

  @Test
  public void intWritable() throws UnexpectedTypeException {
    Converter converter = factory.newConverter(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
    assertThat(converter.toWritableObject(1), is((Object) new IntWritable(1)));
  }

  @Test
  public void longJava() throws UnexpectedTypeException {
    Converter converter = factory.newConverter(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
    assertThat(converter.toJavaObject(new LongWritable(1L)), is((Object) 1L));
  }

  @Test
  public void longWritable() throws UnexpectedTypeException {
    Converter converter = factory.newConverter(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
    assertThat(converter.toWritableObject(1L), is((Object) new LongWritable(1L)));
  }

  @Test
  public void floatJava() throws UnexpectedTypeException {
    Converter converter = factory.newConverter(PrimitiveObjectInspectorFactory.javaFloatObjectInspector);
    assertThat(converter.toJavaObject(new FloatWritable(1.0F)), is((Object) 1.0F));
  }

  @Test
  public void floatWritable() throws UnexpectedTypeException {
    Converter converter = factory.newConverter(PrimitiveObjectInspectorFactory.javaFloatObjectInspector);
    assertThat(converter.toWritableObject(1.0F), is((Object) new FloatWritable(1.0F)));
  }

  @Test
  public void doubleJava() throws UnexpectedTypeException {
    Converter converter = factory.newConverter(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
    assertThat(converter.toJavaObject(new DoubleWritable(1.0D)), is((Object) 1.0D));
  }

  @Test
  public void doubleWritable() throws UnexpectedTypeException {
    Converter converter = factory.newConverter(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
    assertThat(converter.toWritableObject(1.0D), is((Object) new DoubleWritable(1.0D)));
  }

  @Test
  public void timestampJava() throws UnexpectedTypeException {
    Converter converter = factory.newConverter(PrimitiveObjectInspectorFactory.javaTimestampObjectInspector);
    assertThat(converter.toJavaObject(new TimestampWritable(new Timestamp(0L))), is((Object) new Timestamp(0L)));
  }

  @Test
  public void timestampWritable() throws UnexpectedTypeException {
    Converter converter = factory.newConverter(PrimitiveObjectInspectorFactory.javaTimestampObjectInspector);
    assertThat(converter.toWritableObject(new Timestamp(0L)), is((Object) new TimestampWritable(new Timestamp(0L))));
  }

  @Test
  public void dateJava() throws UnexpectedTypeException {
    Converter converter = factory.newConverter(PrimitiveObjectInspectorFactory.javaDateObjectInspector);
    assertThat(converter.toJavaObject(new DateWritable(new Date(0L))).toString(), is((Object) "1970-01-01"));
  }

  @Test
  public void dateWritable() throws UnexpectedTypeException {
    Converter converter = factory.newConverter(PrimitiveObjectInspectorFactory.javaDateObjectInspector);
    assertThat(converter.toWritableObject(new Date(0L)), is((Object) new DateWritable(new Date(0L))));
  }

  @Test
  public void binaryJava() throws UnexpectedTypeException {
    Converter converter = factory.newConverter(PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector);
    assertThat(converter.toJavaObject(new BytesWritable(new byte[] { 0, 1 })), is((Object) new byte[] { 0, 1 }));
  }

  @Test
  public void binaryWritable() throws UnexpectedTypeException {
    Converter converter = factory.newConverter(PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector);
    assertThat(converter.toWritableObject(new byte[] { 0, 1 }), is((Object) new BytesWritable(new byte[] { 0, 1 })));
  }

  private Converter getConverter(TypeInfo typeInfo) {
    ObjectInspector inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
    return factory.newConverter(inspector);
  }

  @Test
  public void charJava() throws UnexpectedTypeException {
    Converter converter = getConverter(TypeInfoFactory.getCharTypeInfo(1));
    HiveChar hiveChar = new HiveChar("a", -1);
    assertThat(converter.toJavaObject(new HiveCharWritable(hiveChar)), is((Object) hiveChar));
  }

  @Test
  public void charWritable() throws UnexpectedTypeException {
    Converter converter = getConverter(TypeInfoFactory.getCharTypeInfo(1));
    HiveChar hiveChar = new HiveChar("a", -1);
    assertThat(converter.toWritableObject(hiveChar), is((Object) new HiveCharWritable(hiveChar)));
  }

  @Test
  public void varcharJava() throws UnexpectedTypeException {
    Converter converter = getConverter(TypeInfoFactory.getVarcharTypeInfo(1));
    HiveVarchar hiveVarchar = new HiveVarchar("a", -1);
    // HiveVarchar doesn't implement equals properly
    assertTrue(((HiveVarchar) converter.toJavaObject(new HiveVarcharWritable(hiveVarchar))).equals(hiveVarchar));
  }

  @Test
  public void varcharWritable() throws UnexpectedTypeException {
    Converter converter = getConverter(TypeInfoFactory.getVarcharTypeInfo(1));
    assertThat(converter.toWritableObject(new HiveVarchar("a", -1)), is((Object) new HiveVarcharWritable(
        new HiveVarchar("a", -1))));
  }

  @Test
  public void decimalJava() throws UnexpectedTypeException {
    Converter converter = getConverter(TypeInfoFactory.getDecimalTypeInfo(2, 1));
    HiveDecimal hiveDecimal = HiveDecimal.create("2.1");
    assertThat(converter.toJavaObject(new HiveDecimalWritable(hiveDecimal)), is((Object) hiveDecimal));
  }

  @Test
  public void decimalWritable() throws UnexpectedTypeException {
    Converter converter = getConverter(TypeInfoFactory.getDecimalTypeInfo(2, 1));
    HiveDecimal hiveDecimal = HiveDecimal.create("2.1");
    assertThat(converter.toWritableObject(hiveDecimal), is((Object) new HiveDecimalWritable(hiveDecimal)));
  }

  @Test
  public void listStringJava() throws UnexpectedTypeException {
    Converter converter = getConverter(TypeInfoFactory.getListTypeInfo(TypeInfoFactory.stringTypeInfo));

    List<Text> writableList = Arrays.asList(new Text("a"));
    List<String> javaList = Arrays.asList("a");

    assertThat(converter.toJavaObject(writableList), is((Object) javaList));
  }

  @Test
  public void listStringWritable() throws UnexpectedTypeException {
    Converter converter = getConverter(TypeInfoFactory.getListTypeInfo(TypeInfoFactory.stringTypeInfo));

    List<Text> writableList = Arrays.asList(new Text("a"));
    List<String> javaList = Arrays.asList("a");

    assertThat(converter.toWritableObject(javaList), is((Object) writableList));
  }

  @Test
  public void mapStringStringJava() throws UnexpectedTypeException {
    TypeInfo typeInfo = TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo);
    Converter converter = getConverter(typeInfo);

    Map<Text, Text> writableMap = new HashMap<>();
    writableMap.put(new Text("hello"), new Text("world"));
    Map<String, String> javaMap = new HashMap<>();
    javaMap.put("hello", "world");

    assertThat(converter.toJavaObject(writableMap), is((Object) javaMap));
  }

  @Test
  public void mapStringStringWritable() throws UnexpectedTypeException {
    TypeInfo typeInfo = TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo);
    Converter converter = getConverter(typeInfo);

    Map<Text, Text> writableMap = new HashMap<>();
    writableMap.put(new Text("hello"), new Text("world"));
    Map<String, String> javaMap = new HashMap<>();
    javaMap.put("hello", "world");

    assertThat(converter.toWritableObject(javaMap), is((Object) writableMap));
  }

  @Test
  public void unionStringLongJava() throws UnexpectedTypeException {
    List<TypeInfo> typeInfos = Arrays.asList((TypeInfo) TypeInfoFactory.stringTypeInfo, TypeInfoFactory.longTypeInfo);
    TypeInfo typeInfo = TypeInfoFactory.getUnionTypeInfo(typeInfos);
    Converter converter = getConverter(typeInfo);

    assertThat(converter.toJavaObject(null), is(nullValue()));
    assertThat(converter.toJavaObject(new StandardUnion((byte) 0, new Text("a"))), is((Object) "a"));
    assertThat(converter.toJavaObject(new StandardUnion((byte) 1, new LongWritable(1L))), is((Object) 1L));

    try {
      converter.toJavaObject(new StandardUnion((byte) 1, new IntWritable(1)));
    } catch (UnexpectedTypeException e) {
      return;
    }
    fail();
  }

  @Test
  public void unionStringLongWritable() throws UnexpectedTypeException {
    List<TypeInfo> typeInfos = Arrays.asList((TypeInfo) TypeInfoFactory.stringTypeInfo, TypeInfoFactory.longTypeInfo);
    TypeInfo typeInfo = TypeInfoFactory.getUnionTypeInfo(typeInfos);
    Converter converter = getConverter(typeInfo);

    assertThat(converter.toWritableObject(null), is(nullValue()));

    UnionObject union;

    union = (UnionObject) converter.toWritableObject("a");
    assertThat(union.getTag(), is((byte) 0));
    assertThat(union.getObject(), is((Object) new Text("a")));

    union = (UnionObject) converter.toWritableObject(1L);
    assertThat(union.getTag(), is((byte) 1));
    assertThat(union.getObject(), is((Object) new LongWritable(1L)));

    try {
      converter.toWritableObject(1);
    } catch (UnexpectedTypeException e) {
      return;
    }
    fail();
  }

  @Test
  public void toJava() throws UnexpectedTypeException {
    StructTypeInfo nested = new StructTypeInfoBuilder().add("char1", TypeInfoFactory.getCharTypeInfo(1)).build();
    TypeInfo typeInfo = new StructTypeInfoBuilder()
        .add("char1", TypeInfoFactory.getCharTypeInfo(1))
        .add("struct_char1", nested)
        .build();

    SettableStructObjectInspector inspector = (SettableStructObjectInspector) OrcStruct.createObjectInspector(typeInfo);
    Object struct = inspector.create();
    inspector.setStructFieldData(struct, inspector.getStructFieldRef("char1"), new HiveCharWritable(new HiveChar("a",
        -1)));

    SettableStructObjectInspector nestedInspector = (SettableStructObjectInspector) OrcStruct
        .createObjectInspector(nested);
    Object nestedStruct = inspector.create();
    nestedInspector.setStructFieldData(nestedStruct, nestedInspector.getStructFieldRef("char1"), new HiveCharWritable(
        new HiveChar("b", -1)));
    inspector.setStructFieldData(struct, inspector.getStructFieldRef("struct_char1"), nestedStruct);

    List<Object> list = new ArrayList<>();
    list.add(new HiveChar("a", -1));
    list.add(Arrays.asList(new HiveChar("b", -1)));

    Converter converter = factory.newConverter(inspector);

    Object convertedList = converter.toJavaObject(struct);
    assertThat(convertedList, is((Object) list));

    Object convertedStruct = converter.toWritableObject(list);
    assertThat(convertedStruct, is(struct));
  }
}
