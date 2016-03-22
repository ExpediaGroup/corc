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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.corc.Corc;
import com.hotels.corc.DefaultConverterFactory;
import com.hotels.corc.StructTypeInfoBuilder;
import com.hotels.corc.test.OrcReader;

@RunWith(MockitoJUnitRunner.class)
public class CorcOutputFormatTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Mock
  private FileSystem fileSystem;
  @Mock
  private Progressable progress;
  @Mock
  private Reporter reporter;

  private final JobConf conf = new JobConf();
  private final CorcOutputFormat outputFormat = new CorcOutputFormat();

  @Test
  public void writer() throws IOException {
    File root = temporaryFolder.getRoot();
    conf.set("mapreduce.output.fileoutputformat.outputdir", root.getCanonicalPath());
    conf.set("mapreduce.task.attempt.id", "attempt_x_0001_m_000001_1");

    String name = "name";
    RecordWriter<NullWritable, Corc> writer = outputFormat.getRecordWriter(fileSystem, conf, name, progress);

    StructTypeInfo typeInfo = new StructTypeInfoBuilder().add("a", TypeInfoFactory.stringTypeInfo).build();

    Corc corc = new Corc(typeInfo, new DefaultConverterFactory());
    corc.set("a", "value");

    writer.write(NullWritable.get(), corc);

    writer.close(reporter);

    Path path = new Path(root.getCanonicalPath() + "/_temporary/0/_temporary/attempt_x_0001_m_000001_1/name");
    try (OrcReader reader = new OrcReader(conf, path)) {
      List<Object> next = reader.next();
      assertThat(next.size(), is(1));
      assertThat(next.get(0), is((Object) "value"));
      assertFalse(reader.hasNext());
    }
    ;
  }
}
