/**
 * Copyright (C) 2015-2016 Expedia Inc.
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
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import cascading.tuple.Fields;

import com.hotels.corc.cascading.SearchArgumentFactory.Builder;

@RunWith(MockitoJUnitRunner.class)
public class SearchArgumentFactoryTest {

  private static final Fields ONE = new Fields("A", Integer.class);
  private static final Fields TWO = new Fields(Fields.names("A", "B"), Fields.types(String.class, Integer.class));

  @Mock
  private org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder mockInternal;

  private SearchArgumentFactory.Builder builder;

  @Before
  public void setup() {
    builder = new Builder(mockInternal);
    when(mockInternal.startNot()).thenReturn(mockInternal);
    when(mockInternal.lessThan(anyString(), any())).thenReturn(mockInternal);
    when(mockInternal.lessThanEquals(anyString(), any())).thenReturn(mockInternal);
    when(mockInternal.end()).thenReturn(mockInternal);
  }

  @Test
  public void between() {
    Builder chain = builder.between(ONE, 1, 2);
    assertThat(chain, is(sameInstance(builder)));
    verify(mockInternal).between("a", 1, 2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void betweenOneThanOneField() {
    builder.between(TWO, 1, 2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void betweenTooFewFields() {
    builder.between(Fields.ALL, 1, 2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void betweenFieldsAreNull() {
    builder.between(null, 1, 2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void betweenArg1TypeIncorrect() {
    builder.between(ONE, "X", 2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void betweenArg2TypeIncorrect() {
    builder.between(ONE, 1, "Y");
  }

  @Test
  public void equals() {
    Builder chain = builder.equals(ONE, 1);
    assertThat(chain, is(sameInstance(builder)));
    verify(mockInternal).equals("a", 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void equalsOneThanOneField() {
    builder.equals(TWO, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void equalsTooFewFields() {
    builder.equals(Fields.ALL, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void equalsFieldsAreNull() {
    builder.equals(null, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void equalsArg1TypeIncorrect() {
    builder.equals(ONE, "X");
  }

  @Test
  public void lessThan() {
    Builder chain = builder.lessThan(ONE, 1);
    assertThat(chain, is(sameInstance(builder)));
    verify(mockInternal).lessThan("a", 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void lessThanOneThanOneField() {
    builder.lessThan(TWO, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void lessThanTooFewFields() {
    builder.lessThan(Fields.ALL, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void lessThanFieldsAreNull() {
    builder.lessThan(null, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void lessThanArg1TypeIncorrect() {
    builder.lessThan(ONE, "X");
  }

  @Test
  public void lessThanEquals() {
    Builder chain = builder.lessThanEquals(ONE, 1);
    assertThat(chain, is(sameInstance(builder)));
    verify(mockInternal).lessThanEquals("a", 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void lessThanEqualsOneThanOneField() {
    builder.lessThanEquals(TWO, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void lessThanEqualsTooFewFields() {
    builder.lessThanEquals(Fields.ALL, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void lessThanEqualsFieldsAreNull() {
    builder.lessThanEquals(null, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void lessThanEqualsArg1TypeIncorrect() {
    builder.lessThanEquals(ONE, "X");
  }

  @Test
  public void greaterThan() {
    Builder chain = builder.greaterThan(ONE, 1);
    assertThat(chain, is(sameInstance(builder)));
    verify(mockInternal).startNot();
    verify(mockInternal).lessThanEquals("a", 1);
    verify(mockInternal).end();
  }

  @Test(expected = IllegalArgumentException.class)
  public void greaterThanOneThanOneField() {
    builder.greaterThan(TWO, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void greaterThanTooFewFields() {
    builder.greaterThan(Fields.ALL, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void greaterThanFieldsAreNull() {
    builder.greaterThan(null, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void greaterThanArg1TypeIncorrect() {
    builder.greaterThan(ONE, "X");
  }

  @Test
  public void greaterThanEquals() {
    Builder chain = builder.greaterThanEquals(ONE, 1);
    assertThat(chain, is(sameInstance(builder)));
    verify(mockInternal).startNot();
    verify(mockInternal).lessThan("a", 1);
    verify(mockInternal).end();
  }

  @Test(expected = IllegalArgumentException.class)
  public void greaterThanEqualsOneThanOneField() {
    builder.greaterThanEquals(TWO, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void greaterThanEqualsTooFewFields() {
    builder.greaterThanEquals(Fields.ALL, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void greaterThanEqualsFieldsAreNull() {
    builder.greaterThanEquals(null, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void greaterThanEqualsArg1TypeIncorrect() {
    builder.greaterThanEquals(ONE, "X");
  }

  @Test
  public void nullSafeEquals() {
    Builder chain = builder.nullSafeEquals(ONE, 1);
    assertThat(chain, is(sameInstance(builder)));
    verify(mockInternal).nullSafeEquals("a", 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullSafeEqualsOneThanOneField() {
    builder.nullSafeEquals(TWO, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullSafeEqualsTooFewFields() {
    builder.nullSafeEquals(Fields.ALL, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullSafeEqualsFieldsAreNull() {
    builder.nullSafeEquals(null, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullSafeEqualsArg1TypeIncorrect() {
    builder.nullSafeEquals(ONE, "X");
  }

  @Test
  public void isNull() {
    Builder chain = builder.isNull(ONE);
    assertThat(chain, is(sameInstance(builder)));
    verify(mockInternal).isNull("a");
  }

  @Test(expected = IllegalArgumentException.class)
  public void isNullPrimitive() {
    builder.isNull(new Fields("A", int.class));
  }

  @Test(expected = IllegalArgumentException.class)
  public void isNullOneThanOneField() {
    builder.isNull(TWO);
  }

  @Test(expected = IllegalArgumentException.class)
  public void isNullTooFewFields() {
    builder.isNull(Fields.ALL);
  }

  @Test(expected = IllegalArgumentException.class)
  public void isNullFieldsAreNull() {
    builder.isNull(null);
  }

  @Test
  public void in() {
    Builder chain = builder.in(ONE, 1, 2, 3);
    assertThat(chain, is(sameInstance(builder)));
    verify(mockInternal).in("a", 1, 2, 3);
  }

  @Test(expected = IllegalArgumentException.class)
  public void inOneThanOneField() {
    builder.in(TWO, 1, 2, 3);
  }

  @Test(expected = IllegalArgumentException.class)
  public void inTooFewFields() {
    builder.in(Fields.ALL, 1, 2, 3);
  }

  @Test(expected = IllegalArgumentException.class)
  public void inFieldsAreNull() {
    builder.in(null, 1, 2, 3);
  }

  @Test(expected = IllegalArgumentException.class)
  public void inArg1TypeIncorrect() {
    builder.in(ONE, "X", 2, 3);
  }

  @Test(expected = IllegalArgumentException.class)
  public void inArg2TypeIncorrect() {
    builder.in(ONE, 1, "Y", 3);
  }

  @Test(expected = IllegalArgumentException.class)
  public void inArg3TypeIncorrect() {
    builder.in(ONE, 1, 2, "Z");
  }

  @Test
  public void startAnd() {
    Builder chain = builder.startAnd();
    assertThat(chain, is(sameInstance(builder)));
    verify(mockInternal).startAnd();
  }

  @Test
  public void startOr() {
    Builder chain = builder.startOr();
    assertThat(chain, is(sameInstance(builder)));
    verify(mockInternal).startOr();
  }

  @Test
  public void startNot() {
    Builder chain = builder.startNot();
    assertThat(chain, is(sameInstance(builder)));
    verify(mockInternal).startNot();
  }

  @Test
  public void end() {
    Builder chain = builder.end();
    assertThat(chain, is(sameInstance(builder)));
    verify(mockInternal).end();
  }

  @Test
  public void build() {
    builder.build();
    verify(mockInternal).build();
  }

  @Test
  public void nameIsLowerCase() {
    assertThat(SearchArgumentFactory.Builder.toName(ONE), is("a"));
  }

  @Test(expected = NullPointerException.class)
  public void nullFieldsForName() {
    SearchArgumentFactory.Builder.toName(null);
  }

  @Test
  public void checkFieldsOk() {
    SearchArgumentFactory.Builder.checkFields(ONE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void checkFieldsNull() {
    SearchArgumentFactory.Builder.checkFields(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void checkFieldsTooLong() {
    SearchArgumentFactory.Builder.checkFields(TWO);
  }

  @Test(expected = IllegalArgumentException.class)
  public void checkFieldsTooShort() {
    SearchArgumentFactory.Builder.checkFields(new Fields());
  }

  @Test(expected = IllegalArgumentException.class)
  public void checkFieldsNoTypes() {
    SearchArgumentFactory.Builder.checkFields(new Fields("A"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void checkFieldsNullType() {
    SearchArgumentFactory.Builder.checkFields(new Fields("A", null));
  }

  @Test(expected = IllegalArgumentException.class)
  public void checkValuesWrongType() {
    SearchArgumentFactory.Builder.checkValueTypes(ONE, "X");
  }

  @Test
  public void checkValuesOk() {
    SearchArgumentFactory.Builder.checkValueTypes(ONE, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void checkValuesPrimitiveCannotBeNull() {
    SearchArgumentFactory.Builder.checkValueTypes(new Fields("A", int.class), (Object) null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void checkMulipleValuesWrongType() {
    SearchArgumentFactory.Builder.checkValueTypes(ONE, 1, 1L);
  }

  @Test
  public void checkMulipleValuesOk() {
    SearchArgumentFactory.Builder.checkValueTypes(ONE, 1, 2);
  }

  @Test
  public void builderFactoryMethod() {
    String kryoFromFields = SearchArgumentFactory
        .newBuilder()
        .startAnd()
        .equals(new Fields("a", String.class), "hello")
        .end()
        .build()
        .toKryo();
    String kryoFromString = org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory
        .newBuilder()
        .startAnd()
        .equals("a", "hello")
        .end()
        .build()
        .toKryo();

    assertThat(kryoFromFields, is(kryoFromString));
  }

}
