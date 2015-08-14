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
package com.hotels.corc.sarg;

import static com.hotels.corc.sarg.EvaluatorFactory.toComparable;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.BINARY;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.BYTE;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.CHAR;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.DATE;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.DECIMAL;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.DOUBLE;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.FLOAT;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.INT;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.LONG;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.SHORT;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.STRING;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.VARCHAR;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf.Operator;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.corc.StructTypeInfoBuilder;

@RunWith(MockitoJUnitRunner.class)
public class EvaluatorFactoryTest {

  private static final String COL0 = "col0";

  @Mock
  private PredicateLeaf predicateLeaf;

  @Test(expected = IllegalArgumentException.class)
  public void notPrimitiveThrowsException() {
    when(predicateLeaf.getColumnName()).thenReturn(COL0);

    StructTypeInfo structTypeInfo = new StructTypeInfoBuilder().add(COL0,
        TypeInfoFactory.getListTypeInfo(TypeInfoFactory.stringTypeInfo)).build();
    EvaluatorFactory factory = new EvaluatorFactory(structTypeInfo);
    factory.newInstance(predicateLeaf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void binaryThrowsException() {
    when(predicateLeaf.getColumnName()).thenReturn(COL0);

    StructTypeInfo structTypeInfo = new StructTypeInfoBuilder().add(COL0, TypeInfoFactory.binaryTypeInfo).build();
    EvaluatorFactory factory = new EvaluatorFactory(structTypeInfo);
    factory.newInstance(predicateLeaf);
  }

  @Test
  public void stringEquals() {
    when(predicateLeaf.getColumnName()).thenReturn(COL0);
    when(predicateLeaf.getOperator()).thenReturn(Operator.EQUALS);
    when(predicateLeaf.getLiteral()).thenReturn("foo");

    StructTypeInfo structTypeInfo = new StructTypeInfoBuilder().add(COL0, TypeInfoFactory.stringTypeInfo).build();
    EvaluatorFactory factory = new EvaluatorFactory(structTypeInfo);
    Evaluator<?> evaluator = factory.newInstance(predicateLeaf);

    assertThat(evaluator, instanceOf(EqualsEvaluator.class));
  }

  @Test
  public void stringNullSafeEquals() {
    when(predicateLeaf.getColumnName()).thenReturn(COL0);
    when(predicateLeaf.getOperator()).thenReturn(Operator.NULL_SAFE_EQUALS);
    when(predicateLeaf.getLiteral()).thenReturn("foo");

    StructTypeInfo structTypeInfo = new StructTypeInfoBuilder().add(COL0, TypeInfoFactory.stringTypeInfo).build();
    EvaluatorFactory factory = new EvaluatorFactory(structTypeInfo);
    Evaluator<?> evaluator = factory.newInstance(predicateLeaf);

    assertThat(evaluator, instanceOf(EqualsEvaluator.class));
  }

  @Test
  public void stringLessThan() {
    when(predicateLeaf.getColumnName()).thenReturn(COL0);
    when(predicateLeaf.getOperator()).thenReturn(Operator.LESS_THAN);
    when(predicateLeaf.getLiteral()).thenReturn("foo");

    StructTypeInfo structTypeInfo = new StructTypeInfoBuilder().add(COL0, TypeInfoFactory.stringTypeInfo).build();
    EvaluatorFactory factory = new EvaluatorFactory(structTypeInfo);
    Evaluator<?> evaluator = factory.newInstance(predicateLeaf);

    assertThat(evaluator, instanceOf(LessThanEvaluator.class));
  }

  @Test
  public void stringLessThanEquals() {
    when(predicateLeaf.getColumnName()).thenReturn(COL0);
    when(predicateLeaf.getOperator()).thenReturn(Operator.LESS_THAN_EQUALS);
    when(predicateLeaf.getLiteral()).thenReturn("foo");

    StructTypeInfo structTypeInfo = new StructTypeInfoBuilder().add(COL0, TypeInfoFactory.stringTypeInfo).build();
    EvaluatorFactory factory = new EvaluatorFactory(structTypeInfo);
    Evaluator<?> evaluator = factory.newInstance(predicateLeaf);

    assertThat(evaluator, instanceOf(LessThanEvaluator.class));
  }

  @Test
  public void stringIn() {
    when(predicateLeaf.getColumnName()).thenReturn(COL0);
    when(predicateLeaf.getOperator()).thenReturn(Operator.IN);
    when(predicateLeaf.getLiteralList()).thenReturn(Arrays.asList((Object) "foo"));

    StructTypeInfo structTypeInfo = new StructTypeInfoBuilder().add(COL0, TypeInfoFactory.stringTypeInfo).build();
    EvaluatorFactory factory = new EvaluatorFactory(structTypeInfo);
    Evaluator<?> evaluator = factory.newInstance(predicateLeaf);

    assertThat(evaluator, instanceOf(InEvaluator.class));
  }

  @Test
  public void stringBetween() {
    when(predicateLeaf.getColumnName()).thenReturn(COL0);
    when(predicateLeaf.getOperator()).thenReturn(Operator.BETWEEN);
    when(predicateLeaf.getLiteralList()).thenReturn(Arrays.asList((Object) "foo", "bar"));

    StructTypeInfo structTypeInfo = new StructTypeInfoBuilder().add(COL0, TypeInfoFactory.stringTypeInfo).build();
    EvaluatorFactory factory = new EvaluatorFactory(structTypeInfo);
    Evaluator<?> evaluator = factory.newInstance(predicateLeaf);

    assertThat(evaluator, instanceOf(BetweenEvaluator.class));
  }

  @Test
  public void stringIsNull() {
    when(predicateLeaf.getColumnName()).thenReturn(COL0);
    when(predicateLeaf.getOperator()).thenReturn(Operator.IS_NULL);

    StructTypeInfo structTypeInfo = new StructTypeInfoBuilder().add(COL0, TypeInfoFactory.stringTypeInfo).build();
    EvaluatorFactory factory = new EvaluatorFactory(structTypeInfo);
    Evaluator<?> evaluator = factory.newInstance(predicateLeaf);

    assertThat(evaluator, instanceOf(IsNullEvaluator.class));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void stringTypical() {
    assertThat(toComparable(STRING, "foo"), is((Comparable) new Text("foo")));
  }

  @Test(expected = ClassCastException.class)
  public void stringWithTextThrowsException() {
    toComparable(STRING, new Text("foo"));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void booleanTypical() {
    assertThat(toComparable(BOOLEAN, true), is((Comparable) new BooleanWritable(true)));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void byteTypical() {
    assertThat(toComparable(BYTE, 0L), is((Comparable) new ByteWritable((byte) 0)));
  }

  @Test(expected = ClassCastException.class)
  public void byteWithByteInput() {
    toComparable(BYTE, (byte) 0);
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void shortTypical() {
    assertThat(toComparable(SHORT, 0L), is((Comparable) new ShortWritable((short) 0)));
  }

  @Test(expected = ClassCastException.class)
  public void shortWithShortInput() {
    toComparable(SHORT, (short) 0);
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void intTypical() {
    assertThat(toComparable(INT, 0L), is((Comparable) new IntWritable(0)));
  }

  @Test(expected = ClassCastException.class)
  public void intWithIntInput() {
    toComparable(INT, 0);
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void longTypical() {
    assertThat(toComparable(LONG, 0L), is((Comparable) new LongWritable(0L)));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void floatTypical() {
    assertThat(toComparable(FLOAT, 0.0D), is((Comparable) new FloatWritable(0.0F)));
  }

  @Test(expected = ClassCastException.class)
  public void floatWithFloatInput() {
    toComparable(FLOAT, 0.0F);
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void doubleTypical() {
    assertThat(toComparable(DOUBLE, 0.0D), is((Comparable) new DoubleWritable(0.0D)));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void timestampTypical() {
    assertThat(toComparable(TIMESTAMP, new Timestamp(0L)), is((Comparable) new TimestampWritable(new Timestamp(0L))));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void dateTypical() {
    assertThat(toComparable(DATE, new DateWritable(new Date(0L))), is((Comparable) new DateWritable(new Date(0L))));
  }

  @Test(expected = ClassCastException.class)
  public void dateWithDateInput() {
    toComparable(DATE, new Date(0L));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void charTypical() {
    assertThat(toComparable(CHAR, "foo"), is((Comparable) new HiveCharWritable(new HiveChar("foo", 3))));
  }

  @Test(expected = ClassCastException.class)
  public void charWithCharInput() {
    toComparable(CHAR, new HiveChar("foo", 3));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void varcharTypical() {
    assertThat(toComparable(VARCHAR, "foo"), is((Comparable) new HiveVarcharWritable(new HiveVarchar("foo", 3))));
  }

  @Test(expected = ClassCastException.class)
  public void varcharWithVarcharInput() {
    toComparable(VARCHAR, new HiveVarchar("foo", 3));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void decimalTypical() {
    assertThat(toComparable(DECIMAL, new BigDecimal("0.0")),
        is((Comparable) new HiveDecimalWritable(HiveDecimal.create(new BigDecimal("0.0")))));
  }

  @Test(expected = ClassCastException.class)
  public void decimalWithDecimalInput() {
    toComparable(DECIMAL, HiveDecimal.create(new BigDecimal("0.0")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void binaryTypical() {
    toComparable(BINARY, new byte[] {});
  }

}
