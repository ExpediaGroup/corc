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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.TruthValue;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.corc.Corc;

@RunWith(MockitoJUnitRunner.class)
public class BetweenEvaluatorTest {

  private static final String COL0 = "col0";

  private static final LongWritable ZERO = new LongWritable(0L);
  private static final LongWritable ONE = new LongWritable(1L);
  private static final LongWritable TWO = new LongWritable(2L);
  private static final LongWritable THREE = new LongWritable(3L);
  private static final LongWritable FOUR = new LongWritable(4L);

  @Mock
  private Corc corc;

  private final Evaluator<?> evaluator = new BetweenEvaluator<>(COL0, ONE, THREE);

  @Test
  public void lessThan() {
    when(corc.getWritable(COL0)).thenReturn(ZERO);
    assertThat(evaluator.evaluate(corc), is(TruthValue.NO));
  }

  @Test
  public void equalsMin() {
    when(corc.getWritable(COL0)).thenReturn(ONE);
    assertThat(evaluator.evaluate(corc), is(TruthValue.YES));
  }

  @Test
  public void middle() {
    when(corc.getWritable(COL0)).thenReturn(TWO);
    assertThat(evaluator.evaluate(corc), is(TruthValue.YES));
  }

  @Test
  public void equalsMax() {
    when(corc.getWritable(COL0)).thenReturn(THREE);
    assertThat(evaluator.evaluate(corc), is(TruthValue.YES));
  }

  @Test
  public void greaterThan() {
    when(corc.getWritable(COL0)).thenReturn(FOUR);
    assertThat(evaluator.evaluate(corc), is(TruthValue.NO));
  }

  @Test
  public void nullInput() {
    when(corc.getWritable(COL0)).thenReturn(null);
    assertThat(evaluator.evaluate(corc), is(TruthValue.NULL));
  }

}
