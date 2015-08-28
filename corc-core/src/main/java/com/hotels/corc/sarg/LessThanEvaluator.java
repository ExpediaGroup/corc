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

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf.Operator;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.TruthValue;
import org.apache.hadoop.io.WritableComparable;

class LessThanEvaluator<T extends WritableComparable<T>> extends Evaluator<T> {

  private final Comparable<T> literal;
  private final int threshold;

  LessThanEvaluator(String fieldName, Comparable<T> literal, Operator operator) {
    super(fieldName);
    this.literal = literal;
    if (operator == Operator.LESS_THAN) {
      threshold = 0;
    } else if (operator == Operator.LESS_THAN_EQUALS) {
      threshold = -1;
    } else {
      throw new IllegalArgumentException("Invalid operator: " + operator);
    }
  }

  @Override
  protected TruthValue evaluate(T value) {
    if (value == null) {
      return TruthValue.NULL;
    }
    if (literal.compareTo(value) > threshold) {
      return TruthValue.YES;
    }
    return TruthValue.NO;
  }

}
