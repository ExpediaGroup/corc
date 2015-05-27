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
package com.hotels.corc.test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.ReaderOptions;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class OrcWriterTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final Configuration conf = new Configuration();

  @Test
  public void typical() throws IOException {
    Path path = new Path(temporaryFolder.getRoot().getCanonicalPath(), "part-00000");

    try (OrcWriter writer = new OrcWriter.Builder(conf, path).addField("a", TypeInfoFactory.stringTypeInfo).build()) {
      writer.addRow("hello");
    }

    ReaderOptions options = OrcFile.readerOptions(conf);
    Reader reader = OrcFile.createReader(path, options);
    RecordReader rows = reader.rows();

    @SuppressWarnings("unchecked")
    List<Object> next = (List<Object>) ObjectInspectorUtils.copyToStandardJavaObject(rows.next(null),
        reader.getObjectInspector());
    assertThat(next.size(), is(1));
    System.out.println(next.get(0).getClass());
    assertThat(next.get(0), is((Object) "hello"));
    assertThat(rows.hasNext(), is(false));

    rows.close();
  }
}
