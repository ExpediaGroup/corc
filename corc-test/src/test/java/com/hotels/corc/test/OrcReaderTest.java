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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.WriterOptions;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class OrcReaderTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final Configuration conf = new Configuration();

  @Test
  public void typical() throws IOException {
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString("struct<a:string>");
    ObjectInspector inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
    WriterOptions options = OrcFile.writerOptions(conf).inspector(inspector);

    Path path = new Path(temporaryFolder.getRoot().getCanonicalPath(), "part-00000");

    Writer writer = OrcFile.createWriter(path, options);
    writer.addRow(Arrays.asList("hello"));
    writer.close();

    try (OrcReader reader = new OrcReader(conf, path)) {
      List<Object> next = reader.next();
      assertThat(next.size(), is(1));
      assertThat(next.get(0), is((Object) "hello"));
      assertThat(reader.hasNext(), is(false));
    }

  }
}
