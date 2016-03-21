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
package com.hotels.corc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.corc.Converter;
import com.hotels.corc.ConverterFactory;
import com.hotels.corc.Corc;
import com.hotels.corc.StructTypeInfoBuilder;
import com.hotels.corc.UnexpectedTypeException;

@RunWith(MockitoJUnitRunner.class)
public class CorcTest {

  private static final String VALUE = "value";

  @Mock
  private ConverterFactory factory;

  @Mock
  private Converter converter;

  private final StructTypeInfo typeInfo = new StructTypeInfoBuilder().add("a", TypeInfoFactory.stringTypeInfo).build();

  private Corc corc;

  @Before
  public void before() {
    when(factory.newConverter(any(ObjectInspector.class))).thenReturn(converter);

    corc = new Corc(typeInfo, factory);
  }

  @Test
  public void set() throws UnexpectedTypeException, IOException {
    when(converter.toWritableObject(VALUE)).thenReturn(new Text(VALUE));

    corc.set("a", VALUE);

    SettableStructObjectInspector inspector = corc.getInspector();
    OrcStruct struct = corc.getOrcStruct();
    StructField structField = inspector.getStructFieldRef("a");
    Object data = inspector.getStructFieldData(struct, structField);

    assertThat(data, is((Object) new Text(VALUE)));
  }

  @Test
  public void setNotExists() throws IOException {
    corc.set("b", VALUE);

    verify(factory, never()).newConverter(any(ObjectInspector.class));
  }

  @Test
  public void get() throws IOException, UnexpectedTypeException {
    when(converter.toJavaObject(new Text(VALUE))).thenReturn(VALUE);

    SettableStructObjectInspector inspector = corc.getInspector();
    OrcStruct struct = corc.getOrcStruct();
    StructField structField = inspector.getStructFieldRef("a");
    inspector.setStructFieldData(struct, structField, new Text(VALUE));

    assertThat(corc.get("a"), is((Object) VALUE));
    // repeat is same
    assertThat(corc.get("a"), is((Object) VALUE));
  }

  @Test
  public void getNotExists() throws IOException {
    StructTypeInfo typeInfo = new StructTypeInfoBuilder().add("a", TypeInfoFactory.stringTypeInfo).build();
    Corc corc = new Corc(typeInfo, factory);

    assertThat(corc.get("b"), is(nullValue()));

    verify(factory, never()).newConverter(any(ObjectInspector.class));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void readFields() throws IOException {
    corc.readFields(null);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void write() throws IOException {
    corc.write(null);
  }

  @Test
  public void recordIdentifier() {
    RecordIdentifier recordIdentifier = new RecordIdentifier();

    corc.setRecordIdentifier(recordIdentifier);

    RecordIdentifier copy = corc.getRecordIdentifier();

    assertThat(copy, is(recordIdentifier));
    assertTrue(copy != recordIdentifier);
  }
}
