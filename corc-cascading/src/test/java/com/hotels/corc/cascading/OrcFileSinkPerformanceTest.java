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
package com.hotels.corc.cascading;

import static org.mockito.Mockito.when;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
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
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.hotels.corc.StructTypeInfoBuilder;

@RunWith(MockitoJUnitRunner.class)
public class OrcFileSinkPerformanceTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Mock
  private FlowProcess<Configuration> flowProcess;

  private StructTypeInfo structTypeInfo;
  private List<Tuple> tuples;
  private Tap<Configuration, ?, ?> tap;

  @Before
  public void before() throws IOException {
    structTypeInfo = createTypeInfo();
    tuples = createTuples();
    tap = createTap();

    when(flowProcess.getConfigCopy()).thenReturn(new Configuration());
  }

  @Test
  public void exerciseScheme() throws IOException {
    TupleEntryCollector collector = tap.openForWrite(flowProcess);
    for (Tuple tuple : tuples) {
      collector.add(tuple);
    }
    collector.close();
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

  private List<Tuple> createTuples() {
    List<Tuple> tuples = new LinkedList<>();
    Tuple tuple = Tuple.size(structTypeInfo.getAllStructFieldNames().size());
    for (int i = 0; i < 1000000; i++) {
      Number n = i;

      tuple.clear();
      tuple.add(n.toString());
      tuple.add(i % 2 == 0);
      tuple.add(n.byteValue());
      tuple.add(n.shortValue());
      tuple.add(i);
      tuple.add(n.longValue());
      tuple.add(n.floatValue());
      tuple.add(n.doubleValue());
      tuple.add(new Timestamp(i));
      tuple.add(new Date(i));
      tuple.add(n.toString().getBytes());
      tuple.add(new BigDecimal(n.toString()));
      tuple.add(Arrays.asList(i));
      tuple.add(createMap(i));
      tuple.add(Arrays.asList(i));
      tuple.add(n.toString());

      tuples.add(tuple);
    }
    return tuples;
  }

  private Map<Object, Object> createMap(int i) {
    Map<Object, Object> map = new HashMap<>();
    map.put(i, i);
    return map;
  }

  private Tap<Configuration, ?, ?> createTap() throws IOException {
    OrcFile orcFile = OrcFile.sink().schema(structTypeInfo).build();
    return new Hfs(orcFile, temporaryFolder.getRoot().getCanonicalPath());
  }

}
