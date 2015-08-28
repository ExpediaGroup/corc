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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.corc.Corc;
import com.hotels.corc.StructTypeInfoBuilder;

@RunWith(MockitoJUnitRunner.class)
public class SearchArgumentFilterTest {

  private static final String COL0 = "col0";
  private static final String COL1 = "col1";

  private static final long ZERO = 0L;
  private static final long ONE = 1L;
  private static final long TWO = 2L;
  private static final long THREE = 3L;
  private static final long FOUR = 4L;

  private static final LongWritable ZERO_WRITABLE = new LongWritable(ZERO);
  private static final LongWritable ONE_WRITABLE = new LongWritable(ONE);
  private static final LongWritable TWO_WRITABLE = new LongWritable(TWO);
  private static final LongWritable THREE_WRITABLE = new LongWritable(THREE);
  private static final LongWritable FOUR_WRITABLE = new LongWritable(FOUR);

  @Mock
  private Corc corc;

  private final Builder builder = SearchArgumentFactory.newBuilder();
  private final StructTypeInfo structTypeInfo = new StructTypeInfoBuilder()
      .add(COL0, TypeInfoFactory.longTypeInfo)
      .add(COL1, TypeInfoFactory.longTypeInfo)
      .build();

  @Test
  public void equalsIsTrue() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ZERO_WRITABLE);
    SearchArgument searchArgument = builder.startAnd().equals(COL0, ZERO).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(true));
  }

  @Test
  public void equalsIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ZERO_WRITABLE);
    SearchArgument searchArgument = builder.startAnd().equals(COL0, ONE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void equalsNullIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(null);
    SearchArgument searchArgument = builder.startAnd().equals(COL0, ZERO).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void notEqualsIsTrue() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ZERO_WRITABLE);
    SearchArgument searchArgument = builder.startNot().equals(COL0, ONE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(true));
  }

  @Test
  public void notEqualsIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ZERO_WRITABLE);
    SearchArgument searchArgument = builder.startNot().equals(COL0, ZERO).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void notEqualsNullIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(null);
    SearchArgument searchArgument = builder.startNot().equals(COL0, ZERO).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void nullSafeEqualsIsTrue() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ZERO_WRITABLE);
    SearchArgument searchArgument = builder.startAnd().nullSafeEquals(COL0, ZERO).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(true));
  }

  @Test
  public void nullSafeEqualsIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ZERO_WRITABLE);
    SearchArgument searchArgument = builder.startAnd().nullSafeEquals(COL0, ONE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void nullSafeEqualsNullIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(null);
    SearchArgument searchArgument = builder.startAnd().nullSafeEquals(COL0, ZERO).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void notNullSafeEqualsIsTrue() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ZERO_WRITABLE);
    SearchArgument searchArgument = builder.startNot().nullSafeEquals(COL0, ONE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(true));
  }

  @Test
  public void notNullSafeEqualsIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ZERO_WRITABLE);
    SearchArgument searchArgument = builder.startNot().nullSafeEquals(COL0, ZERO).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void notNullSafeEqualsNullIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(null);
    SearchArgument searchArgument = builder.startNot().nullSafeEquals(COL0, ZERO).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void isNullIsTrue() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(null);
    SearchArgument searchArgument = builder.startAnd().isNull(COL0).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(true));
  }

  @Test
  public void isNullIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ZERO_WRITABLE);
    SearchArgument searchArgument = builder.startAnd().isNull(COL0).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void notIsNullIsTrue() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ZERO_WRITABLE);
    SearchArgument searchArgument = builder.startNot().isNull(COL0).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(true));
  }

  @Test
  public void notIsNullIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(null);
    SearchArgument searchArgument = builder.startNot().isNull(COL0).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void inIsTrue() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ZERO_WRITABLE);
    SearchArgument searchArgument = builder.startAnd().in(COL0, ZERO).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(true));
  }

  @Test
  public void inIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ZERO_WRITABLE);
    SearchArgument searchArgument = builder.startAnd().in(COL0, ONE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void inNullIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(null);
    SearchArgument searchArgument = builder.startAnd().in(COL0, ZERO).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void notInIsTrue() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ZERO_WRITABLE);
    SearchArgument searchArgument = builder.startNot().in(COL0, ONE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(true));
  }

  @Test
  public void notInIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ZERO_WRITABLE);
    SearchArgument searchArgument = builder.startNot().in(COL0, ZERO).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void notInNullIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(null);
    SearchArgument searchArgument = builder.startNot().in(COL0, ZERO).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void betweenLessThanIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ZERO_WRITABLE);
    SearchArgument searchArgument = builder.startAnd().between(COL0, ONE, THREE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void betweenMinIsTrue() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ONE_WRITABLE);
    SearchArgument searchArgument = builder.startAnd().between(COL0, ONE, THREE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(true));
  }

  @Test
  public void betweenMiddleIsTrue() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(TWO_WRITABLE);
    SearchArgument searchArgument = builder.startAnd().between(COL0, ONE, THREE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(true));
  }

  @Test
  public void betweenMaxIsTrue() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(THREE_WRITABLE);
    SearchArgument searchArgument = builder.startAnd().between(COL0, ONE, THREE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(true));
  }

  @Test
  public void betweenGreaterThanIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(FOUR_WRITABLE);
    SearchArgument searchArgument = builder.startAnd().between(COL0, ONE, THREE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void betweenNullIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(null);
    SearchArgument searchArgument = builder.startAnd().between(COL0, ONE, THREE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void notBetweenLessThanIsTrue() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ZERO_WRITABLE);
    SearchArgument searchArgument = builder.startNot().between(COL0, ONE, THREE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(true));
  }

  @Test
  public void notBetweenMinIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ONE_WRITABLE);
    SearchArgument searchArgument = builder.startNot().between(COL0, ONE, THREE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void notBetweenMiddleIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(TWO_WRITABLE);
    SearchArgument searchArgument = builder.startNot().between(COL0, ONE, THREE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void notBetweenMaxIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(THREE_WRITABLE);
    SearchArgument searchArgument = builder.startNot().between(COL0, ONE, THREE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void notBetweenGreaterThanIsTrue() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(FOUR_WRITABLE);
    SearchArgument searchArgument = builder.startNot().between(COL0, ONE, THREE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(true));
  }

  @Test
  public void notBetweenNullIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(null);
    SearchArgument searchArgument = builder.startNot().between(COL0, ONE, THREE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void lessThanIsLessThanIsTrue() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ZERO_WRITABLE);
    SearchArgument searchArgument = builder.startAnd().lessThan(COL0, ONE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(true));
  }

  @Test
  public void lessThanIsEqualIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ONE_WRITABLE);
    SearchArgument searchArgument = builder.startAnd().lessThan(COL0, ONE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void lessThanIsGreaterThanIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(TWO_WRITABLE);
    SearchArgument searchArgument = builder.startAnd().lessThan(COL0, ONE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void lessThanNullIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(null);
    SearchArgument searchArgument = builder.startAnd().lessThan(COL0, ONE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void notLessThanIsLessThanIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ZERO_WRITABLE);
    SearchArgument searchArgument = builder.startNot().lessThan(COL0, ONE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void notLessThanIsEqualIsTrue() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ONE_WRITABLE);
    SearchArgument searchArgument = builder.startNot().lessThan(COL0, ONE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(true));
  }

  @Test
  public void notLessThanIsGreaterThanIsTrue() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(TWO_WRITABLE);
    SearchArgument searchArgument = builder.startNot().lessThan(COL0, ONE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(true));
  }

  @Test
  public void notLessThanNullIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(null);
    SearchArgument searchArgument = builder.startNot().lessThan(COL0, ONE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void lessThanEqualsIsLessThanIsTrue() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ZERO_WRITABLE);
    SearchArgument searchArgument = builder.startAnd().lessThanEquals(COL0, ONE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(true));
  }

  @Test
  public void lessThanEqualsIsEqualIsTrue() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ONE_WRITABLE);
    SearchArgument searchArgument = builder.startAnd().lessThanEquals(COL0, ONE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(true));
  }

  @Test
  public void lessThanEqualsIsGreaterThanIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(TWO_WRITABLE);
    SearchArgument searchArgument = builder.startAnd().lessThanEquals(COL0, ONE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void lessThanEqualsNullIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(null);
    SearchArgument searchArgument = builder.startAnd().lessThanEquals(COL0, ONE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void notLessThanEqualsIsLessThanIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ZERO_WRITABLE);
    SearchArgument searchArgument = builder.startNot().lessThanEquals(COL0, ONE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void notLessThanEqualsIsEqualIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ONE_WRITABLE);
    SearchArgument searchArgument = builder.startNot().lessThanEquals(COL0, ONE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void notLessThanEqualsIsGreaterThanIsTrue() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(TWO_WRITABLE);
    SearchArgument searchArgument = builder.startNot().lessThanEquals(COL0, ONE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(true));
  }

  @Test
  public void notLessThanEqualsNullIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(null);
    SearchArgument searchArgument = builder.startNot().lessThanEquals(COL0, ONE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void orEqualsIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ZERO_WRITABLE);
    when(corc.getWritable(COL1)).thenReturn(ZERO_WRITABLE);
    SearchArgument searchArgument = builder.startOr().equals(COL0, ONE).equals(COL1, ONE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void orEqualsIsTrue() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ZERO_WRITABLE);
    when(corc.getWritable(COL1)).thenReturn(ONE_WRITABLE);
    SearchArgument searchArgument = builder.startOr().equals(COL0, ONE).equals(COL1, ONE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(true));
  }

  @Test
  public void andEqualsIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ZERO_WRITABLE);
    when(corc.getWritable(COL1)).thenReturn(ONE_WRITABLE);
    SearchArgument searchArgument = builder.startAnd().equals(COL0, ONE).equals(COL1, ONE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

  @Test
  public void andEqualsIsTrue() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ONE_WRITABLE);
    when(corc.getWritable(COL1)).thenReturn(ONE_WRITABLE);
    SearchArgument searchArgument = builder.startAnd().equals(COL0, ONE).equals(COL1, ONE).end().build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(true));
  }

  @Test
  public void complexArgumentIsTrue() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ONE_WRITABLE);
    when(corc.getWritable(COL1)).thenReturn(ZERO_WRITABLE);

    /*
     * COL0 is not null and COL0 > 0 and COL1 not between 1 and 3
     */
    SearchArgument searchArgument = builder
        .startAnd()
        .startNot()
        .isNull(COL0)
        .end()
        .startNot()
        .lessThanEquals(COL0, ZERO)
        .end()
        .startNot()
        .between(COL1, ONE, THREE)
        .end()
        .end()
        .build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(true));
  }

  @Test
  public void complexArgumentIsFalse() throws IOException {
    when(corc.getWritable(COL0)).thenReturn(ONE_WRITABLE);
    when(corc.getWritable(COL1)).thenReturn(TWO_WRITABLE);

    /*
     * COL0 is not null and COL0 > 0 and COL1 not between 1 and 3
     */
    SearchArgument searchArgument = builder
        .startAnd()
        .startNot()
        .isNull(COL0)
        .end()
        .startNot()
        .lessThanEquals(COL0, ZERO)
        .end()
        .startNot()
        .between(COL1, ONE, THREE)
        .end()
        .end()
        .build();
    assertThat(new SearchArgumentFilter(searchArgument, structTypeInfo).accept(corc), is(false));
  }

}
