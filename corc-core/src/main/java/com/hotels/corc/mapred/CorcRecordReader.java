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
package com.hotels.corc.mapred;

import java.io.IOException;

import org.apache.hadoop.hive.ql.io.AcidInputFormat.AcidRecordReader;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordReader;

import com.hotels.corc.ConverterFactory;
import com.hotels.corc.Corc;
import com.hotels.corc.Filter;

/**
 * A wrapper for {@link OrcRecordReader} exposing {@link Corc} in place of {@link OrcStruct}.
 */
class CorcRecordReader implements RecordReader<NullWritable, Corc> {
  private final StructTypeInfo typeInfo;
  private final RecordReader<NullWritable, OrcStruct> reader;
  private final ConverterFactory factory;
  private final Filter filter;
  private final AcidRecordReader<NullWritable, OrcStruct> transactionalReader;
  private final boolean transactional;

  CorcRecordReader(StructTypeInfo typeInfo, RecordReader<NullWritable, OrcStruct> reader, ConverterFactory factory,
      Filter filter) {
    this.typeInfo = typeInfo;
    this.reader = reader;
    this.factory = factory;
    this.filter = filter;
    transactional = AcidRecordReader.class.isAssignableFrom(reader.getClass());
    if (transactional) {
      transactionalReader = (AcidRecordReader<NullWritable, OrcStruct>) reader;
    } else {
      transactionalReader = null;
    }
  }

  @Override
  public boolean next(NullWritable key, Corc value) throws IOException {
    while (reader.next(key, value.getOrcStruct())) {
      if (filter.accept(value)) {
        if (transactional) {
          value.setRecordIdentifier(transactionalReader.getRecordIdentifier());
        }
        return true;
      }
    }
    return false;
  }

  @Override
  public NullWritable createKey() {
    return reader.createKey();
  }

  @Override
  public Corc createValue() {
    return new Corc(typeInfo, factory);
  }

  @Override
  public long getPos() throws IOException {
    return reader.getPos();
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public float getProgress() throws IOException {
    return reader.getProgress();
  }

}