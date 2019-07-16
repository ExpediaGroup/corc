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
package com.hotels.corc.mapred;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.hive.ql.io.AcidInputFormat.AcidRecordReader;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.corc.ConverterFactory;
import com.hotels.corc.Corc;
import com.hotels.corc.Filter;

@RunWith(MockitoJUnitRunner.class)
public class CorcRecordReaderTest {

  private final StructTypeInfo typeInfo = (StructTypeInfo) TypeInfoUtils.getTypeInfoFromTypeString("struct<a:string>");

  @Mock
  private ConverterFactory factory;

  @Test
  public void readerCreateKey() {
    @SuppressWarnings("unchecked")
    RecordReader<NullWritable, OrcStruct> recordReader = mock(RecordReader.class);
    CorcRecordReader reader = new CorcRecordReader(typeInfo, recordReader, factory, Filter.ACCEPT);

    reader.createKey();
    verify(recordReader).createKey();
  }

  @Test
  public void readerCreateValue() {
    @SuppressWarnings("unchecked")
    RecordReader<NullWritable, OrcStruct> recordReader = mock(RecordReader.class);
    CorcRecordReader reader = new CorcRecordReader(typeInfo, recordReader, factory, Filter.ACCEPT);

    Corc corc = reader.createValue();
    verify(recordReader, never()).createValue();

    assertThat(corc.getInspector().getTypeName(), is("struct<a:string>"));

    Object create = ((SettableStructObjectInspector) OrcStruct.createObjectInspector(typeInfo)).create();
    assertThat(corc.getOrcStruct(), is(create));
  }

  @Test
  public void readerGetPos() throws IOException {
    @SuppressWarnings("unchecked")
    RecordReader<NullWritable, OrcStruct> recordReader = mock(RecordReader.class);
    CorcRecordReader reader = new CorcRecordReader(typeInfo, recordReader, factory, Filter.ACCEPT);

    reader.getPos();
    verify(recordReader).getPos();
  }

  @Test
  public void readerGetProgress() throws IOException {
    @SuppressWarnings("unchecked")
    RecordReader<NullWritable, OrcStruct> recordReader = mock(RecordReader.class);
    CorcRecordReader reader = new CorcRecordReader(typeInfo, recordReader, factory, Filter.ACCEPT);

    reader.getProgress();
    verify(recordReader).getProgress();
  }

  @Test
  public void readerClose() throws IOException {
    @SuppressWarnings("unchecked")
    RecordReader<NullWritable, OrcStruct> recordReader = mock(RecordReader.class);
    CorcRecordReader reader = new CorcRecordReader(typeInfo, recordReader, factory, Filter.ACCEPT);

    reader.close();
    verify(recordReader).close();
  }

  @Test
  public void readerNext() throws IOException {
    @SuppressWarnings("unchecked")
    RecordReader<NullWritable, OrcStruct> recordReader = mock(RecordReader.class);
    CorcRecordReader reader = new CorcRecordReader(typeInfo, recordReader, factory, Filter.ACCEPT);

    Corc corc = mock(Corc.class);

    when(recordReader.next(any(NullWritable.class), any(OrcStruct.class))).thenReturn(true);

    boolean next = reader.next(NullWritable.get(), corc);

    assertTrue(next);

    verify(corc, never()).setRecordIdentifier(any(RecordIdentifier.class));
  }

  @Test
  public void readerNoNext() throws IOException {
    @SuppressWarnings("unchecked")
    RecordReader<NullWritable, OrcStruct> recordReader = mock(RecordReader.class);
    CorcRecordReader reader = new CorcRecordReader(typeInfo, recordReader, factory, Filter.ACCEPT);

    Corc corc = mock(Corc.class);

    when(recordReader.next(any(NullWritable.class), any(OrcStruct.class))).thenReturn(false);

    boolean next = reader.next(NullWritable.get(), corc);

    assertFalse(next);

    verify(corc, never()).setRecordIdentifier(any(RecordIdentifier.class));
  }

  @Test
  public void readerNoNextFilterNotAccept() throws IOException {
    @SuppressWarnings("unchecked")
    RecordReader<NullWritable, OrcStruct> recordReader = mock(RecordReader.class);

    Filter filter = mock(Filter.class);
    CorcRecordReader reader = new CorcRecordReader(typeInfo, recordReader, factory, filter);

    Corc corc = mock(Corc.class);

    when(recordReader.next(any(NullWritable.class), any(OrcStruct.class))).thenReturn(true).thenReturn(false);
    when(filter.accept(corc)).thenReturn(false);

    boolean next = reader.next(NullWritable.get(), corc);

    assertFalse(next);

    verify(corc, never()).setRecordIdentifier(any(RecordIdentifier.class));
  }

  @Test
  public void readerNextTransactional() throws IOException {
    @SuppressWarnings("unchecked")
    AcidRecordReader<NullWritable, OrcStruct> recordReader = mock(AcidRecordReader.class);
    CorcRecordReader reader = new CorcRecordReader(typeInfo, recordReader, factory, Filter.ACCEPT);

    Corc corc = mock(Corc.class);

    when(recordReader.next(any(NullWritable.class), any(OrcStruct.class))).thenReturn(true);

    boolean next = reader.next(NullWritable.get(), corc);

    assertTrue(next);

    verify(corc).setRecordIdentifier(any(RecordIdentifier.class));
  }

  @Test
  public void readerNoNextTransactional() throws IOException {
    @SuppressWarnings("unchecked")
    AcidRecordReader<NullWritable, OrcStruct> recordReader = mock(AcidRecordReader.class);
    CorcRecordReader reader = new CorcRecordReader(typeInfo, recordReader, factory, Filter.ACCEPT);

    Corc corc = mock(Corc.class);

    when(recordReader.next(any(NullWritable.class), any(OrcStruct.class))).thenReturn(false);

    boolean next = reader.next(NullWritable.get(), corc);

    assertFalse(next);

    verify(corc, never()).setRecordIdentifier(any(RecordIdentifier.class));
  }
}
