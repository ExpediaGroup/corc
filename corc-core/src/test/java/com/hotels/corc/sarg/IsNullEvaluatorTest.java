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

import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.TruthValue;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.corc.Corc;

@RunWith(MockitoJUnitRunner.class)
public class IsNullEvaluatorTest {

  private static final String COL0 = "col0";

  private static final Text FOO = new Text("foo");

  @Mock
  private Corc corc;

  @SuppressWarnings("rawtypes")
  private final Evaluator<?> evaluator = new IsNullEvaluator(COL0);

  @Test
  public void stringIsNotNull() {
    when(corc.getWritable(COL0)).thenReturn(FOO);
    assertThat(evaluator.evaluate(corc), is(TruthValue.NO));
  }

  @Test
  public void stringIsNull() {
    when(corc.getWritable(COL0)).thenReturn(null);
    assertThat(evaluator.evaluate(corc), is(TruthValue.YES));
  }

}
