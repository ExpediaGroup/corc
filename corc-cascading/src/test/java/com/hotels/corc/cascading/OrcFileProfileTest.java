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
package com.hotels.corc.cascading;

import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.hotels.corc.StructTypeInfoBuilder;

@RunWith(MockitoJUnitRunner.class)
public class OrcFileProfileTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @SuppressWarnings("rawtypes")
  @Mock
  private FlowProcess flowProcess;

  private final JobConf conf = new JobConf();

  @Ignore
  @Test
  public void profileTest() throws IOException {
    StructTypeInfo structTypeInfo = new StructTypeInfoBuilder().add("a", TypeInfoFactory.stringTypeInfo).build();

    OrcFile orcFile = OrcFile.sink().schema(structTypeInfo).build();
    Tap<?, ?, ?> tap = new Hfs(orcFile, temporaryFolder.getRoot().getCanonicalPath());

    when(flowProcess.getConfigCopy()).thenReturn(conf);

    @SuppressWarnings("unchecked")
    TupleEntryCollector collector = tap.openForWrite(flowProcess);

    long start = System.currentTimeMillis();
    Tuple tuple = new Tuple("hello");
    for (int i = 0; i < 10000000; i++) {
      collector.add(tuple);
    }
    System.out.println(System.currentTimeMillis() - start);
    collector.close();
  }
}
