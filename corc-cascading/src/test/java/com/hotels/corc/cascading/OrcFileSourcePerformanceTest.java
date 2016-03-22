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
package com.hotels.corc.cascading;

import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.TupleEntryIterator;

import com.hotels.corc.StructTypeInfoBuilder;
import com.hotels.corc.test.OrcWriter;

@RunWith(MockitoJUnitRunner.class)
public class OrcFileSourcePerformanceTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Mock
  private FlowProcess<Configuration> flowProcess;

  private StructTypeInfo structTypeInfo;
  private Tap<Configuration, ?, ?> tap;

  @Before
  public void before() throws IOException {
    structTypeInfo = createTypeInfo();
    writeOrcFile();
    tap = createTap();

    when(flowProcess.getConfigCopy()).thenReturn(new JobConf());
  }

  @Test
  public void exerciseScheme() throws IOException {
    TupleEntryIterator iterator = tap.openForRead(flowProcess);
    while (iterator.hasNext()) {
      iterator.next();
    }
    iterator.close();
  }

  private StructTypeInfo createTypeInfo() {
    return new StructTypeInfoBuilder()
        .add("a", TypeInfoFactory.stringTypeInfo)
        .add("b", TypeInfoFactory.booleanTypeInfo)
        .add("c", TypeInfoFactory.byteTypeInfo)
        .add("d", TypeInfoFactory.shortTypeInfo)
        .add("e", TypeInfoFactory.intTypeInfo)
        .add("f", TypeInfoFactory.longTypeInfo)
        .add("g", TypeInfoFactory.floatTypeInfo)
        .add("h", TypeInfoFactory.doubleTypeInfo)
        .add("i", TypeInfoFactory.timestampTypeInfo)
        .add("j", TypeInfoFactory.dateTypeInfo)
        .add("k", TypeInfoFactory.binaryTypeInfo)
        .add("l", TypeInfoFactory.decimalTypeInfo)
        .add("m", TypeInfoFactory.getListTypeInfo(TypeInfoFactory.intTypeInfo))
        .add("n", TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.intTypeInfo, TypeInfoFactory.intTypeInfo))
        .add("o", new StructTypeInfoBuilder().add("a", TypeInfoFactory.intTypeInfo).build())
        .add("p", TypeInfoFactory.getUnionTypeInfo(Arrays.asList((TypeInfo) TypeInfoFactory.stringTypeInfo)))
        .build();
  }

  private void writeOrcFile() throws IOException {
    Path path = new Path(temporaryFolder.getRoot().getCanonicalPath(), "part-00000");
    List<Object> struct = new ArrayList<>(structTypeInfo.getAllStructFieldNames().size());
    try (OrcWriter writer = new OrcWriter(new Configuration(), path, structTypeInfo)) {
      for (int i = 0; i < 1000000; i++) {
        Number n = i;

        struct.clear();
        struct.add(n.toString());
        struct.add(i % 2 == 0);
        struct.add(n.byteValue());
        struct.add(n.shortValue());
        struct.add(i);
        struct.add(n.longValue());
        struct.add(n.floatValue());
        struct.add(n.doubleValue());
        struct.add(new Timestamp(i));
        struct.add(new Date(i));
        struct.add(n.toString().getBytes());
        struct.add(HiveDecimal.create(n.toString()));
        struct.add(Arrays.asList(i));
        struct.add(createMap(i));
        struct.add(Arrays.asList(i));
        struct.add(new StandardUnion((byte) 0, n.toString()));

        writer.addRow(struct);
      }
    }
  }

  private Map<Object, Object> createMap(int i) {
    Map<Object, Object> map = new HashMap<>();
    map.put(i, i);
    return map;
  }

  private Tap<Configuration, ?, ?> createTap() throws IOException {
    OrcFile orcFile = OrcFile.source().columns(structTypeInfo).schemaFromFile().build();
    return new Hfs(orcFile, temporaryFolder.getRoot().getCanonicalPath());
  }

}
