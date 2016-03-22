/**
 * Copyright (C) 2015-2016 Expedia Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.corc.cascading;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.math.BigDecimal;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.junit.Test;

import com.hotels.corc.Converter;
import com.hotels.corc.ConverterFactory;
import com.hotels.corc.UnexpectedTypeException;

public class CascadingConverterFactoryTest {

  private final ConverterFactory factory = new CascadingConverterFactory();

  private Converter getConverter(TypeInfo typeInfo) {
    ObjectInspector inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
    return factory.newConverter(inspector);
  }

  @Test
  public void charJava() throws UnexpectedTypeException {
    Converter converter = getConverter(TypeInfoFactory.getCharTypeInfo(1));
    HiveChar hiveChar = new HiveChar("a", -1);
    assertThat(converter.toJavaObject(new HiveCharWritable(hiveChar)), is((Object) "a"));
  }

  @Test
  public void charWritable() throws UnexpectedTypeException {
    Converter converter = getConverter(TypeInfoFactory.getCharTypeInfo(1));
    HiveChar hiveChar = new HiveChar("a", -1);
    assertThat(converter.toWritableObject("a"), is((Object) new HiveCharWritable(hiveChar)));
  }

  @Test
  public void varcharJava() throws UnexpectedTypeException {
    Converter converter = getConverter(TypeInfoFactory.getVarcharTypeInfo(1));
    HiveVarchar hiveVarchar = new HiveVarchar("a", -1);
    // HiveVarchar doesn't implement equals properly
    assertThat(converter.toJavaObject(new HiveVarcharWritable(hiveVarchar)), is((Object) "a"));
  }

  @Test
  public void varcharWritable() throws UnexpectedTypeException {
    Converter converter = getConverter(TypeInfoFactory.getVarcharTypeInfo(1));
    assertThat(converter.toWritableObject("a"), is((Object) new HiveVarcharWritable(new HiveVarchar("a", -1))));
  }

  @Test
  public void decimalJava() throws UnexpectedTypeException {
    Converter converter = getConverter(TypeInfoFactory.getDecimalTypeInfo(2, 1));
    HiveDecimal hiveDecimal = HiveDecimal.create("2.1");
    assertThat(converter.toJavaObject(new HiveDecimalWritable(hiveDecimal)), is((Object) new BigDecimal("2.1")));
  }

  @Test
  public void decimalWritable() throws UnexpectedTypeException {
    Converter converter = getConverter(TypeInfoFactory.getDecimalTypeInfo(2, 1));
    HiveDecimal hiveDecimal = HiveDecimal.create("2.1");
    assertThat(converter.toWritableObject("2.1"), is((Object) new HiveDecimalWritable(hiveDecimal)));
    assertThat(converter.toWritableObject(new BigDecimal("2.1")), is((Object) new HiveDecimalWritable(hiveDecimal)));
  }

}
