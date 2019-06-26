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
package com.hotels.corc.sarg;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

class EvaluatorFactory {

  private final StructTypeInfo structTypeInfo;

  EvaluatorFactory(StructTypeInfo structTypeInfo) {
    this.structTypeInfo = structTypeInfo;
  }

  Evaluator<?> newInstance(PredicateLeaf predicateLeaf) {
    TypeInfo typeInfo = structTypeInfo.getStructFieldTypeInfo(predicateLeaf.getColumnName());
    if (typeInfo.getCategory() != Category.PRIMITIVE) {
      throw new IllegalArgumentException("Unsupported column type: " + typeInfo.getCategory());
    }
    PrimitiveCategory category = ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
    if (category == PrimitiveCategory.BINARY) {
      throw new IllegalArgumentException("Unsupported column type: " + category);
    }

    switch (predicateLeaf.getOperator()) {
    case EQUALS:
    case NULL_SAFE_EQUALS:
      return equalsEvaluator(predicateLeaf, category);
    case LESS_THAN:
    case LESS_THAN_EQUALS:
      return lessThanEvaluator(predicateLeaf, category);
    case IN:
      return inEvaluator(predicateLeaf, category);
    case BETWEEN:
      return betweenEvaluator(predicateLeaf, category);
    case IS_NULL:
      return isNullEvaluator(predicateLeaf);
    default:
      throw new IllegalArgumentException("Unsupported operator: " + predicateLeaf.getOperator());
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private Evaluator<?> equalsEvaluator(PredicateLeaf predicateLeaf, PrimitiveCategory category) {
    Comparable<?> literal = toComparable(category, predicateLeaf.getLiteral());
    return new EqualsEvaluator(predicateLeaf.getColumnName(), literal);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private Evaluator<?> lessThanEvaluator(PredicateLeaf predicateLeaf, PrimitiveCategory category) {
    Comparable<?> literal = toComparable(category, predicateLeaf.getLiteral());
    return new LessThanEvaluator(predicateLeaf.getColumnName(), literal, predicateLeaf.getOperator());
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private Evaluator<?> inEvaluator(PredicateLeaf predicateLeaf, PrimitiveCategory category) {
    List<Comparable<?>> literals = new ArrayList<>(predicateLeaf.getLiteralList().size());
    for (Object literalItem : predicateLeaf.getLiteralList()) {
      literals.add(toComparable(category, literalItem));
    }
    return new InEvaluator(predicateLeaf.getColumnName(), literals);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private Evaluator<?> betweenEvaluator(PredicateLeaf predicateLeaf, PrimitiveCategory category) {
    List<Object> literalList = predicateLeaf.getLiteralList();
    Comparable<?> minLiteral = toComparable(category, literalList.get(0));
    Comparable<?> maxLiteral = toComparable(category, literalList.get(1));
    return new BetweenEvaluator(predicateLeaf.getColumnName(), minLiteral, maxLiteral);
  }

  @SuppressWarnings("rawtypes")
  private Evaluator<?> isNullEvaluator(PredicateLeaf predicateLeaf) {
    return new IsNullEvaluator(predicateLeaf.getColumnName());
  }

  static Comparable<?> toComparable(PrimitiveCategory category, Object literal) {
    String stringLiteral;
    switch (category) {
    case STRING:
      return new Text((String) literal);
    case BOOLEAN:
      return new BooleanWritable((Boolean) literal);
    case BYTE:
      return new ByteWritable(((Long) literal).byteValue());
    case SHORT:
      return new ShortWritable(((Long) literal).shortValue());
    case INT:
      return new IntWritable(((Long) literal).intValue());
    case LONG:
      return new LongWritable((Long) literal);
    case FLOAT:
      return new FloatWritable(((Double) literal).floatValue());
    case DOUBLE:
      return new DoubleWritable((Double) literal);
    case TIMESTAMP:
      return new TimestampWritable((Timestamp) literal);
    case DATE:
      return (DateWritable) literal;
    case CHAR:
      stringLiteral = (String) literal;
      return new HiveCharWritable(new HiveChar(stringLiteral, stringLiteral.length()));
    case VARCHAR:
      stringLiteral = (String) literal;
      return new HiveVarcharWritable(new HiveVarchar(stringLiteral, stringLiteral.length()));
    case DECIMAL:
      return new HiveDecimalWritable(HiveDecimal.create((BigDecimal) literal));
    default:
      throw new IllegalArgumentException("Unsupported category: " + category);
    }
  }

}
