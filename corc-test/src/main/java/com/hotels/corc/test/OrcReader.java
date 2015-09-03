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
package com.hotels.corc.test;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

public class OrcReader implements Closeable {

  private final ObjectInspector inspector;
  private final RecordReader rows;

  public OrcReader(Configuration conf, Path path) throws IOException {
    Reader reader = OrcFile.createReader(path.getFileSystem(conf), path);
    inspector = reader.getObjectInspector();
    rows = reader.rows();
  }

  public boolean hasNext() throws IOException {
    return rows.hasNext();
  }

  @SuppressWarnings("unchecked")
  public List<Object> next() throws IOException {
    return (List<Object>) ObjectInspectorUtils.copyToStandardJavaObject(rows.next(null), inspector);
  }

  @Override
  public void close() throws IOException {
    rows.close();
  }

}