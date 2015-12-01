/**
 * Copyright (C) 2015 Expedia Inc.
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
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Test;

import cascading.tuple.Fields;

import com.hotels.corc.StructTypeInfoBuilder;

public class SchemaFactoryTest {

  @Test
  public void createFields() {
    StructTypeInfo typeInfo = new StructTypeInfoBuilder()
        .add("a", TypeInfoFactory.stringTypeInfo)
        .add("b", TypeInfoFactory.binaryTypeInfo)
        .add("c", TypeInfoFactory.getCharTypeInfo(1))
        .add("d", TypeInfoFactory.getVarcharTypeInfo(1))
        .add("e", TypeInfoFactory.getDecimalTypeInfo(1, 0))
        .add("f", TypeInfoFactory.getListTypeInfo(TypeInfoFactory.stringTypeInfo))
        .add("g", TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo))
        .add("h", TypeInfoFactory
            .getUnionTypeInfo(Arrays.asList((TypeInfo) TypeInfoFactory.stringTypeInfo, TypeInfoFactory.intTypeInfo)))
        .add("i", new StructTypeInfoBuilder().add("x", TypeInfoFactory.stringTypeInfo).build())
        .build();

    Fields fields = SchemaFactory.newFields(typeInfo);

    Class<?>[] types = fields.getTypesClasses();
    assertThat(types[0], is((Object) String.class));
    assertThat(types[1], is((Object) byte[].class));
    assertThat(types[2], is((Object) Object.class));
    assertThat(types[3], is((Object) Object.class));
    assertThat(types[4], is((Object) Object.class));
    assertThat(types[5], is((Object) Object.class));
    assertThat(types[6], is((Object) Object.class));
    assertThat(types[7], is((Object) Object.class));
    assertThat(types[8], is((Object) Object.class));
  }

  @Test
  public void createStructTypeInfo() {
    String[] names = new String[] { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k" };
    Class<?>[] types = new Class<?>[] { String.class, Boolean.class, Byte.class, Short.class, Integer.class, Long.class,
        Float.class, Double.class, Timestamp.class, Date.class, byte[].class };

    Fields fields = new Fields(names, types);

    StructTypeInfo typeInfo = SchemaFactory.newStructTypeInfo(fields);

    assertThat(typeInfo.getStructFieldTypeInfo("a"), is((TypeInfo) TypeInfoFactory.stringTypeInfo));
    assertThat(typeInfo.getStructFieldTypeInfo("b"), is((TypeInfo) TypeInfoFactory.booleanTypeInfo));
    assertThat(typeInfo.getStructFieldTypeInfo("c"), is((TypeInfo) TypeInfoFactory.byteTypeInfo));
    assertThat(typeInfo.getStructFieldTypeInfo("d"), is((TypeInfo) TypeInfoFactory.shortTypeInfo));
    assertThat(typeInfo.getStructFieldTypeInfo("e"), is((TypeInfo) TypeInfoFactory.intTypeInfo));
    assertThat(typeInfo.getStructFieldTypeInfo("f"), is((TypeInfo) TypeInfoFactory.longTypeInfo));
    assertThat(typeInfo.getStructFieldTypeInfo("g"), is((TypeInfo) TypeInfoFactory.floatTypeInfo));
    assertThat(typeInfo.getStructFieldTypeInfo("h"), is((TypeInfo) TypeInfoFactory.doubleTypeInfo));
    assertThat(typeInfo.getStructFieldTypeInfo("i"), is((TypeInfo) TypeInfoFactory.timestampTypeInfo));
    assertThat(typeInfo.getStructFieldTypeInfo("j"), is((TypeInfo) TypeInfoFactory.dateTypeInfo));
    assertThat(typeInfo.getStructFieldTypeInfo("k"), is((TypeInfo) TypeInfoFactory.binaryTypeInfo));
  }

  @Test(expected = NullPointerException.class)
  public void createStructTypeInfoComplex() {
    String[] names = new String[] { "a" };
    Class<?>[] types = new Class<?>[] { List.class };

    Fields fields = new Fields(names, types);

    SchemaFactory.newStructTypeInfo(fields);
  }

  @Test
  public void bigDecimalField() throws Exception {
    Fields bigDecimalField = new Fields("bigD", BigDecimal.class);
    StructTypeInfo typeInfo = SchemaFactory.newStructTypeInfo(bigDecimalField);
    assertThat(typeInfo.getStructFieldTypeInfo("bigD"), is((TypeInfo) TypeInfoFactory.decimalTypeInfo));
  }
}
