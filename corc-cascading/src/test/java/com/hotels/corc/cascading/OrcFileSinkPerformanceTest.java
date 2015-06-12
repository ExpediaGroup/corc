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

import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
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
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.hotels.corc.StructTypeInfoBuilder;

@RunWith(MockitoJUnitRunner.class)
public class OrcFileSinkPerformanceTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Mock
  private FlowProcess<JobConf> flowProcess;

  private StructTypeInfo structTypeInfo;
  private List<Tuple> tuples;
  private Tap<JobConf, ?, ?> tap;

  @Before
  public void before() throws IOException {
    structTypeInfo = createTypeInfo();
    tuples = createTuples();
    tap = createTap();

    when(flowProcess.getConfigCopy()).thenReturn(new JobConf());
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

      tuples.add(tuple);
    }
    return tuples;
  }

  private Map<Object, Object> createMap(int i) {
    Map<Object, Object> map = new HashMap<>();
    map.put(i, i);
    return map;
  }

  private Tap<JobConf, ?, ?> createTap() throws IOException {
    OrcFile orcFile = OrcFile.sink().schema(structTypeInfo).build();
    return new Hfs(orcFile, temporaryFolder.getRoot().getCanonicalPath());
  }

}