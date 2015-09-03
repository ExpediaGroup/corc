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
package com.hotels.corc;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Test;

public class StructTypeInfoBuilderTest {

  @Test
  public void allTypes() {
    StructTypeInfo structTypeInfo = new StructTypeInfoBuilder()
        .add("col0", TypeInfoFactory.stringTypeInfo)
        .add("col1", TypeInfoFactory.booleanTypeInfo)
        .add("col2", TypeInfoFactory.byteTypeInfo)
        .add("col3", TypeInfoFactory.shortTypeInfo)
        .add("col4", TypeInfoFactory.intTypeInfo)
        .add("col5", TypeInfoFactory.longTypeInfo)
        .add("col6", TypeInfoFactory.floatTypeInfo)
        .add("col7", TypeInfoFactory.doubleTypeInfo)
        .add("col8", TypeInfoFactory.timestampTypeInfo)
        .add("col9", TypeInfoFactory.dateTypeInfo)
        .add("col10", TypeInfoFactory.getCharTypeInfo(1))
        .add("col11", TypeInfoFactory.getVarcharTypeInfo(1))
        .add("col12", TypeInfoFactory.getDecimalTypeInfo(2, 1))
        .add("col13", TypeInfoFactory.getListTypeInfo(TypeInfoFactory.stringTypeInfo))
        .add("col14", TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo))
        .add("col15", new StructTypeInfoBuilder().add("col0", TypeInfoFactory.stringTypeInfo).build())
        .build();

    assertThat(structTypeInfo.getTypeName(),
        is(new StringBuilder()
            .append("struct<")
            .append("col0:string,")
            .append("col1:boolean,")
            .append("col2:tinyint,")
            .append("col3:smallint,")
            .append("col4:int,")
            .append("col5:bigint,")
            .append("col6:float,")
            .append("col7:double,")
            .append("col8:timestamp,")
            .append("col9:date,")
            .append("col10:char(1),")
            .append("col11:varchar(1),")
            .append("col12:decimal(2,1),")
            .append("col13:array<string>,")
            .append("col14:map<string,string>,")
            .append("col15:struct<col0:string>")
            .append(">")
            .toString()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void duplicateName() {
    new StructTypeInfoBuilder().add("col0", TypeInfoFactory.stringTypeInfo).add("col0", TypeInfoFactory.stringTypeInfo);
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullName() {
    new StructTypeInfoBuilder().add(null, TypeInfoFactory.stringTypeInfo);
  }

  @Test(expected = IllegalArgumentException.class)
  public void emptyName() {
    new StructTypeInfoBuilder().add("", TypeInfoFactory.stringTypeInfo);
  }

  @Test(expected = IllegalArgumentException.class)
  public void whitespaceName() {
    new StructTypeInfoBuilder().add(" ", TypeInfoFactory.stringTypeInfo);
  }

  @Test(expected = IllegalArgumentException.class)
  public void tabName() {
    new StructTypeInfoBuilder().add("\t", TypeInfoFactory.stringTypeInfo);
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullTypeInfo() {
    new StructTypeInfoBuilder().add("col0", null);
  }
}
