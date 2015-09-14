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
package com.hotels.corc.sarg;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf.Operator;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.TruthValue;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.corc.Corc;

@RunWith(MockitoJUnitRunner.class)
public class LessThanEvaluatorTest {

  private static final String COL0 = "col0";

  private static final LongWritable ZERO = new LongWritable(0L);
  private static final LongWritable ONE = new LongWritable(1L);
  private static final LongWritable TWO = new LongWritable(2L);

  @Mock
  private Corc corc;

  private Evaluator<?> evaluator;

  @Test
  public void lessThanIsLessThan() {
    when(corc.getWritable(COL0)).thenReturn(ZERO);
    evaluator = new LessThanEvaluator<>(COL0, ONE, Operator.LESS_THAN);
    assertThat(evaluator.evaluate(corc), is(TruthValue.YES));
  }

  @Test
  public void lessThanIsEqualInput() {
    when(corc.getWritable(COL0)).thenReturn(ONE);
    evaluator = new LessThanEvaluator<>(COL0, ONE, Operator.LESS_THAN);
    assertThat(evaluator.evaluate(corc), is(TruthValue.NO));
  }

  @Test
  public void lessThatIsGreaterThan() {
    when(corc.getWritable(COL0)).thenReturn(TWO);
    evaluator = new LessThanEvaluator<>(COL0, ONE, Operator.LESS_THAN);
    assertThat(evaluator.evaluate(corc), is(TruthValue.NO));
  }

  @Test
  public void lessThanIsNullInput() {
    when(corc.getWritable(COL0)).thenReturn(null);
    evaluator = new LessThanEvaluator<>(COL0, ONE, Operator.LESS_THAN);
    assertThat(evaluator.evaluate(corc), is(TruthValue.NULL));
  }

  @Test
  public void lessThanEqualIsLessThan() {
    when(corc.getWritable(COL0)).thenReturn(ZERO);
    evaluator = new LessThanEvaluator<>(COL0, ONE, Operator.LESS_THAN_EQUALS);
    assertThat(evaluator.evaluate(corc), is(TruthValue.YES));
  }

  @Test
  public void lessThanEqualIsEqualInput() {
    when(corc.getWritable(COL0)).thenReturn(ONE);
    evaluator = new LessThanEvaluator<>(COL0, ONE, Operator.LESS_THAN_EQUALS);
    assertThat(evaluator.evaluate(corc), is(TruthValue.YES));
  }

  @Test
  public void lessThatEqualIsGreaterThan() {
    when(corc.getWritable(COL0)).thenReturn(TWO);
    evaluator = new LessThanEvaluator<>(COL0, ONE, Operator.LESS_THAN_EQUALS);
    assertThat(evaluator.evaluate(corc), is(TruthValue.NO));
  }

  @Test
  public void lessThanEqualIsNullInput() {
    when(corc.getWritable(COL0)).thenReturn(null);
    evaluator = new LessThanEvaluator<>(COL0, ONE, Operator.LESS_THAN_EQUALS);
    assertThat(evaluator.evaluate(corc), is(TruthValue.NULL));
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidOperator() {
    evaluator = new LessThanEvaluator<>(COL0, ONE, Operator.EQUALS);
  }

}
