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

import java.io.IOException;

import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import com.hotels.corc.Corc;

/**
 * A wrapper for {@link OrcRecordWriter} exposing {@link Corc} in place of {@link OrcStruct}.
 */
class CorcRecordWriter implements RecordWriter<NullWritable, Corc> {
  @SuppressWarnings("rawtypes")
  private final RecordWriter writer; // OrcSerdeRow is package private so have to use rawtypes

  CorcRecordWriter(RecordWriter<NullWritable, ?> writer) {
    this.writer = writer;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void write(NullWritable key, Corc value) throws IOException {
    writer.write(key, value.serialize());
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    writer.close(reporter);
  }

}