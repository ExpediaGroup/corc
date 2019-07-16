/**
 * Copyright (C) 2015-2019 Expedia Inc.
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

import cascading.tuple.Fields;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

import static com.hotels.corc.cascading.FieldsTypeUtils.toType;

public class FieldsTypeUtilsTest {

  @Test
  public void intToLongType(){
    Fields fields = new Fields("A", Integer.class);
    PredicateLeaf.Type fieldsType = toType(fields);

    assertThat(fieldsType, is (PredicateLeaf.Type.LONG));
  }


  @Test
  public void longToLongType(){
    Fields fields = new Fields("A", Long.class);
    PredicateLeaf.Type fieldsType = toType(fields);

    assertThat(fieldsType, is (PredicateLeaf.Type.LONG));
  }

  @Test
  public void doubleToFloatType(){
    Fields fields = new Fields("A", Double.class);
    PredicateLeaf.Type fieldsType = toType(fields);

    assertThat(fieldsType, is (PredicateLeaf.Type.FLOAT));
  }

  @Test
  public void dateToDateType(){
    Fields fields = new Fields("A", Date.class);
    PredicateLeaf.Type fieldsType = toType(fields);

    assertThat(fieldsType, is (PredicateLeaf.Type.DATE));
  }

  @Test
  public void stringToStringType(){
    Fields fields = new Fields("A", String.class);
    PredicateLeaf.Type fieldsType = toType(fields);

    assertThat(fieldsType, is (PredicateLeaf.Type.STRING));
  }

  @Test
  public void hiveCharToStringType(){
    Fields fields = new Fields("A", HiveChar.class);
    PredicateLeaf.Type fieldsType = toType(fields);

    assertThat(fieldsType, is (PredicateLeaf.Type.STRING));
  }

  @Test
  public void hiveVarcharToStringType(){
    Fields fields = new Fields("A", HiveVarchar.class);
    PredicateLeaf.Type fieldsType = toType(fields);

    assertThat(fieldsType, is (PredicateLeaf.Type.STRING));
  }

  @Test
  public void hiveDecimalWritableToDecimalType(){
    Fields fields = new Fields("A", HiveDecimalWritable.class);
    PredicateLeaf.Type fieldsType = toType(fields);

    assertThat(fieldsType, is (PredicateLeaf.Type.DECIMAL));
  }

  @Test
  public void timestampToTimestampType(){
    Fields fields = new Fields("A", Timestamp.class);
    PredicateLeaf.Type fieldsType = toType(fields);

    assertThat(fieldsType, is (PredicateLeaf.Type.TIMESTAMP));
  }

  @Test
  public void booleanToBooleanType(){
    Fields fields = new Fields("A", Boolean.class);
    PredicateLeaf.Type fieldsType = toType(fields);

    assertThat(fieldsType, is (PredicateLeaf.Type.BOOLEAN));
  }

  @Test (expected = IllegalStateException.class)
  public void incorrectDecimalType(){
    Fields fields = new Fields("A", BigDecimal.class);
    PredicateLeaf.Type fieldsType = toType(fields);
  }

}
