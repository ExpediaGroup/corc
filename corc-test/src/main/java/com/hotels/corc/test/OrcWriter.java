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
package com.hotels.corc.test;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.WriterOptions;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class OrcWriter implements Closeable {

  private final Writer writer;

  public OrcWriter(Configuration conf, Path path, StructTypeInfo typeInfo) throws IOException {
    ObjectInspector inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
    WriterOptions writerOptions = OrcFile.writerOptions(conf).inspector(inspector);
    writer = OrcFile.createWriter(path, writerOptions);
  }

  public void addRow(Object... values) throws IOException {
    addRow(Arrays.asList(values));
  }

  public void addRow(List<Object> struct) throws IOException {
    writer.addRow(struct);
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  public static class Builder {

    private final Configuration conf;
    private final Path path;
    private final List<String> names = new ArrayList<String>();
    private final List<TypeInfo> typeInfos = new ArrayList<TypeInfo>();

    public Builder(Configuration conf, Path path) {
      this.conf = conf;
      this.path = path;
    }

    public OrcWriter.Builder addField(String name, TypeInfo typeInfo) {
      names.add(name);
      typeInfos.add(typeInfo);
      return this;
    }

    public OrcWriter build() throws IOException {
      StructTypeInfo typeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(names, typeInfos);
      return new OrcWriter(conf, path, typeInfo);
    }

  }

}