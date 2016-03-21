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
package com.hotels.corc.mapred;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.corc.Corc;

@RunWith(MockitoJUnitRunner.class)
public class CorcRecordWriterTest {

  @SuppressWarnings("rawtypes")
  @Mock
  private RecordWriter writer;

  @SuppressWarnings("unchecked")
  @Test
  public void decoratedWriterWrite() throws IOException {
    CorcRecordWriter corcRecordWriter = new CorcRecordWriter(writer);

    NullWritable key = mock(NullWritable.class);
    Corc value = mock(Corc.class);
    Object serialized = mock(Object.class);

    when(value.serialize()).thenReturn(serialized);

    corcRecordWriter.write(key, value);

    verify(writer).write(key, serialized);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void decoratedWriterClose() throws IOException {
    CorcRecordWriter corcRecordWriter = new CorcRecordWriter(writer);

    Reporter reporter = mock(Reporter.class);

    corcRecordWriter.close(reporter);

    verify(writer).close(reporter);
  }

}
