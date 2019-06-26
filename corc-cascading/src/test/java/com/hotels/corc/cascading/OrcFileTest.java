/**
 * Copyright (C) 2015-2019 Expedia Inc.
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

import static cascading.tuple.Fields.names;
import static cascading.tuple.Fields.types;
import static com.hotels.plunger.asserts.PlungerAssert.tupleEntryList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.flow.tez.Hadoop2TezFlowConnector;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleException;

import com.hotels.corc.StructTypeInfoBuilder;
import com.hotels.corc.test.OrcReader;
import com.hotels.corc.test.OrcWriter;
import com.hotels.plunger.Data;
import com.hotels.plunger.DataBuilder;
import com.hotels.plunger.Plunger;

public class OrcFileTest {

  private static final Fields FIELD_A = new Fields("A", String.class);
  private static final Fields FIELD_B = new Fields("B", String.class);
  private static final Fields FIELDS_AB = FIELD_A.append(FIELD_B);

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final JobConf conf = new JobConf();

  private String path;

  @Before
  public void before() throws IOException {
    path = temporaryFolder.newFolder("data").getCanonicalPath();
  }

  private void write(TypeInfo typeInfo, List<Object> values) throws IOException {
    StructTypeInfo structTypeInfo = new StructTypeInfoBuilder().add("a", typeInfo).build();

    OrcFile orcFile = OrcFile.sink().schema(structTypeInfo).build();
    Fields fields = orcFile.getSinkFields();

    DataBuilder builder = new DataBuilder(fields);
    for (Object value : values) {
      builder.addTuple(value);
    }
    Data data = builder.build();
    Tap<?, ?, ?> tap = new Hfs(orcFile, path);

    Plunger.writeData(data).toTap(tap);
  }

  private OrcReader getOrcReader() throws IOException {
    return new OrcReader(conf, new Path(path, "part-00000"));
  }

  private List<Tuple> read(TypeInfo typeInfo) throws IOException {
    StructTypeInfo structTypeInfo = new StructTypeInfoBuilder().add("a", typeInfo).build();

    OrcFile orcFile = OrcFile.source().columns(structTypeInfo).schemaFromFile().build();
    Tap<?, ?, ?> tap = new Hfs(orcFile, path);

    return Plunger.readDataFromTap(tap).asTupleList();
  }

  private OrcWriter getOrcWriter(TypeInfo typeInfo) throws IOException {
    return new OrcWriter.Builder(conf, new Path(path, "part-00000")).addField("a", typeInfo).build();
  }

  @Test
  public void writeString() throws IOException {
    List<Object> values = new ArrayList<>();
    values.add("hello");
    values.add(null);

    write(TypeInfoFactory.stringTypeInfo, values);

    try (OrcReader reader = getOrcReader()) {
      assertThat(reader.hasNext(), is(true));
      assertThat(reader.next().get(0), is((Object) "hello"));

      assertThat(reader.hasNext(), is(true));
      assertThat(reader.next().get(0), is(nullValue()));

      assertThat(reader.hasNext(), is(false));
    }
  }

  @Test
  public void readString() throws IOException {
    TypeInfo typeInfo = TypeInfoFactory.stringTypeInfo;

    try (OrcWriter writer = getOrcWriter(typeInfo)) {
      writer.addRow("hello");
      writer.addRow((Object) null);
    }

    List<Tuple> list = read(typeInfo);
    assertThat(list.size(), is(2));
    assertThat(list.get(0).getObject(0), is((Object) "hello"));
    assertThat(list.get(1).getObject(0), is(nullValue()));
  }

  @Test
  public void writeChar() throws IOException {
    List<Object> values = new ArrayList<>();
    values.add("hello");
    values.add(new HiveChar("world", 1));
    values.add(null);

    write(TypeInfoFactory.getCharTypeInfo(1), values);

    try (OrcReader reader = getOrcReader()) {
      assertThat(reader.hasNext(), is(true));
      assertThat(((HiveChar) reader.next().get(0)).getValue(), is("h"));

      assertThat(reader.hasNext(), is(true));
      assertThat(((HiveChar) reader.next().get(0)).getValue(), is("w"));

      assertThat(reader.hasNext(), is(true));
      assertThat(reader.next().get(0), is(nullValue()));

      assertThat(reader.hasNext(), is(false));
    }
  }

  @Test
  public void readChar() throws IOException {
    TypeInfo typeInfo = TypeInfoFactory.getCharTypeInfo(1);

    try (OrcWriter writer = getOrcWriter(typeInfo)) {
      writer.addRow(new HiveChar("hello", 1));
      writer.addRow((Object) null);
    }

    List<Tuple> list = read(typeInfo);
    assertThat(list.size(), is(2));
    assertThat(list.get(0).getObject(0), is((Object) "h"));
    assertThat(list.get(1).getObject(0), is(nullValue()));
  }

  @Test
  public void writeVarchar() throws IOException {
    List<Object> values = new ArrayList<>();
    values.add("hello");
    values.add(new HiveVarchar("world", 1));
    values.add(null);

    write(TypeInfoFactory.getVarcharTypeInfo(1), values);

    try (OrcReader reader = getOrcReader()) {
      assertThat(reader.hasNext(), is(true));
      assertThat(((HiveVarchar) reader.next().get(0)).getValue(), is("h"));

      assertThat(reader.hasNext(), is(true));
      assertThat(((HiveVarchar) reader.next().get(0)).getValue(), is("w"));

      assertThat(reader.hasNext(), is(true));
      assertThat(reader.next().get(0), is(nullValue()));

      assertThat(reader.hasNext(), is(false));
    }
  }

  @Test
  public void readVarchar() throws IOException {
    TypeInfo typeInfo = TypeInfoFactory.getVarcharTypeInfo(1);

    try (OrcWriter writer = getOrcWriter(typeInfo)) {
      writer.addRow(new HiveVarchar("hello", 1));
      writer.addRow((Object) null);
    }

    List<Tuple> list = read(typeInfo);
    assertThat(list.size(), is(2));
    assertThat(list.get(0).getObject(0), is((Object) "h"));
    assertThat(list.get(1).getObject(0), is(nullValue()));
  }

  @Test
  public void writeDecimal() throws IOException {
    List<Object> values = new ArrayList<>();
    values.add(HiveDecimal.create(new BigDecimal("1.23")));
    values.add(new BigDecimal("2.34"));
    values.add("3.45");
    values.add(null);

    write(TypeInfoFactory.getDecimalTypeInfo(2, 1), values);

    try (OrcReader reader = getOrcReader()) {
      assertThat(reader.hasNext(), is(true));
      assertThat(((HiveDecimal) reader.next().get(0)).bigDecimalValue(), is(new BigDecimal("1.2")));

      assertThat(reader.hasNext(), is(true));
      assertThat(((HiveDecimal) reader.next().get(0)).bigDecimalValue(), is(new BigDecimal("2.3")));

      assertThat(reader.hasNext(), is(true));
      assertThat(((HiveDecimal) reader.next().get(0)).bigDecimalValue(), is(new BigDecimal("3.5")));

      assertThat(reader.hasNext(), is(true));
      assertThat(reader.next().get(0), is(nullValue()));

      assertThat(reader.hasNext(), is(false));
    }
  }

  @Test
  public void readDecimal() throws IOException {
    TypeInfo typeInfo = TypeInfoFactory.getDecimalTypeInfo(2, 1);

    try (OrcWriter writer = getOrcWriter(typeInfo)) {
      writer.addRow(HiveDecimal.create(new BigDecimal("1.2")));
      writer.addRow((Object) null);
    }

    List<Tuple> list = read(typeInfo);
    assertThat(list.size(), is(2));
    assertThat(list.get(0).getObject(0), is((Object) new BigDecimal("1.2")));
    assertThat(list.get(1).getObject(0), is(nullValue()));
  }

  @Test
  public void writeListString() throws IOException {
    List<Object> values = new ArrayList<>();
    values.add(Arrays.asList("hello"));
    values.add(null);

    write(TypeInfoFactory.getListTypeInfo(TypeInfoFactory.stringTypeInfo), values);

    try (OrcReader reader = getOrcReader()) {
      assertThat(reader.hasNext(), is(true));
      assertThat(reader.next().get(0), is((Object) Arrays.asList("hello")));

      assertThat(reader.hasNext(), is(true));
      assertThat(reader.next().get(0), is(nullValue()));

      assertThat(reader.hasNext(), is(false));
    }
  }

  @Test
  public void readListString() throws IOException {
    TypeInfo typeInfo = TypeInfoFactory.getListTypeInfo(TypeInfoFactory.stringTypeInfo);

    try (OrcWriter writer = getOrcWriter(typeInfo)) {
      writer.addRow(Arrays.asList("hello"));
      writer.addRow((Object) null);
    }

    List<Tuple> list = read(typeInfo);
    assertThat(list.size(), is(2));
    assertThat(list.get(0).getObject(0), is((Object) Arrays.asList("hello")));
    assertThat(list.get(1).getObject(0), is(nullValue()));
  }

  @Test
  public void writeMapStringString() throws IOException {
    Map<Object, Object> map = new HashMap<>();
    map.put("hello", "world");

    List<Object> values = new ArrayList<>();
    values.add(map);
    values.add(null);

    write(TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo), values);

    try (OrcReader reader = getOrcReader()) {
      assertThat(reader.hasNext(), is(true));
      assertThat(reader.next().get(0), is((Object) map));

      assertThat(reader.hasNext(), is(true));
      assertThat(reader.next().get(0), is(nullValue()));

      assertThat(reader.hasNext(), is(false));
    }
  }

  @Test
  public void readMapStringString() throws IOException {
    TypeInfo typeInfo = TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo);

    Map<Object, Object> map = new HashMap<>();
    map.put("hello", "world");

    try (OrcWriter writer = getOrcWriter(typeInfo)) {
      writer.addRow(map);
      writer.addRow((Object) null);
    }

    List<Tuple> list = read(typeInfo);
    assertThat(list.size(), is(2));
    assertThat(list.get(0).getObject(0), is((Object) map));
    assertThat(list.get(1).getObject(0), is(nullValue()));
  }

  @Test
  public void writeMapCharString() throws IOException {
    Map<Object, Object> map = new HashMap<>();
    map.put("hello", "world");
    map.put("hi", "world");

    List<Object> values = new ArrayList<>();
    values.add(map);
    values.add(null);

    write(TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.getCharTypeInfo(1), TypeInfoFactory.stringTypeInfo), values);

    Map<Object, Object> expected = new HashMap<>();
    expected.put(new HiveChar("h", 1), "world");

    try (OrcReader reader = getOrcReader()) {
      assertThat(reader.hasNext(), is(true));
      assertThat(reader.next().get(0), is((Object) expected));

      assertThat(reader.hasNext(), is(true));
      assertThat(reader.next().get(0), is(nullValue()));

      assertThat(reader.hasNext(), is(false));
    }
  }

  @Test
  public void readMapCharString() throws IOException {
    TypeInfo typeInfo = TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.getCharTypeInfo(1),
        TypeInfoFactory.stringTypeInfo);

    Map<Object, Object> map = new HashMap<>();
    map.put(new HiveChar("h", 1), "world");

    try (OrcWriter writer = getOrcWriter(typeInfo)) {
      writer.addRow(map);
      writer.addRow((Object) null);
    }

    Map<Object, Object> expected = new HashMap<>();
    expected.put("h", "world");

    List<Tuple> list = read(typeInfo);
    assertThat(list.size(), is(2));
    assertThat(list.get(0).getObject(0), is((Object) expected));
    assertThat(list.get(1).getObject(0), is(nullValue()));
  }

  @Test
  public void writeStructString() throws IOException {
    List<Object> struct = new ArrayList<>();
    struct.add("hello");

    List<Object> values = new ArrayList<>();
    values.add(struct);
    values.add(null);

    write(new StructTypeInfoBuilder().add("b", TypeInfoFactory.stringTypeInfo).build(), values);

    try (OrcReader reader = getOrcReader()) {
      assertThat(reader.hasNext(), is(true));
      assertThat(reader.next().get(0), is((Object) struct));

      assertThat(reader.hasNext(), is(true));
      assertThat(reader.next().get(0), is(nullValue()));

      assertThat(reader.hasNext(), is(false));
    }
  }

  @Test
  public void readStructString() throws IOException {
    TypeInfo typeInfo = new StructTypeInfoBuilder().add("b", TypeInfoFactory.stringTypeInfo).build();

    List<Object> struct = new ArrayList<>();
    struct.add("hello");

    try (OrcWriter writer = getOrcWriter(typeInfo)) {
      writer.addRow((Object) struct);
      writer.addRow((Object) null);
    }

    List<Tuple> list = read(typeInfo);
    assertThat(list.size(), is(2));
    assertThat(list.get(0).getObject(0), is((Object) struct));
    assertThat(list.get(1).getObject(0), is(nullValue()));
  }

  @Test
  public void writeStructChar() throws IOException {
    List<Object> struct1 = new ArrayList<>();
    struct1.add("hello");

    List<Object> struct2 = new ArrayList<>();
    struct2.add(new HiveChar("world", 1));

    List<Object> struct3 = new ArrayList<>();
    struct3.add(null);

    List<Object> values = new ArrayList<>();
    values.add(struct1);
    values.add(struct2);
    values.add(struct3);
    values.add(null);

    write(new StructTypeInfoBuilder().add("b", TypeInfoFactory.getCharTypeInfo(1)).build(), values);

    try (OrcReader reader = getOrcReader()) {
      assertThat(reader.hasNext(), is(true));
      assertThat(((HiveChar) ((List) reader.next().get(0)).get(0)).getValue(), is("h"));

      assertThat(reader.hasNext(), is(true));
      assertThat(((HiveChar) ((List) reader.next().get(0)).get(0)).getValue(), is("w"));

      assertThat(reader.hasNext(), is(true));
      assertThat(((List) reader.next().get(0)).get(0), is(nullValue()));

      assertThat(reader.hasNext(), is(true));
      assertThat(reader.next().get(0), is(nullValue()));

      assertThat(reader.hasNext(), is(false));
    }
  }

  @Test
  public void readStructChar() throws IOException {
    TypeInfo typeInfo = new StructTypeInfoBuilder().add("b", TypeInfoFactory.getCharTypeInfo(1)).build();

    List<Object> struct1 = new ArrayList<>();
    struct1.add(new HiveChar("hello", 1));

    List<Object> struct2 = new ArrayList<>();
    struct2.add(new HiveChar("world", 1));

    List<Object> struct3 = new ArrayList<>();
    struct3.add(null);

    try (OrcWriter writer = getOrcWriter(typeInfo)) {
      writer.addRow((Object) struct1);
      writer.addRow((Object) struct2);
      writer.addRow((Object) struct3);
      writer.addRow((Object) null);
    }

    List<Tuple> list = read(typeInfo);
    assertThat(list.size(), is(4));
    assertThat(list.get(0).getObject(0), is((Object) Arrays.asList("h")));
    assertThat(list.get(1).getObject(0), is((Object) Arrays.asList("w")));
    assertThat(list.get(2).getObject(0), is((Object) Arrays.asList((Object) null)));
    assertThat(list.get(3).getObject(0), is(nullValue()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void missingFieldsTypes() {
    OrcFile.source().declaredFields(new Fields("A")).schemaFromFile().build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullFieldsTypes() {
    OrcFile.source().declaredFields(new Fields("A", null)).schemaFromFile().build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void duplicateFields() {
    Fields upperA = new Fields("A", String.class);
    Fields lowerA = new Fields("a", String.class);
    OrcFile.source().declaredFields(upperA.append(lowerA)).schemaFromFile().build();
  }

  @Test
  public void writeTypical() throws IOException {
    Data data = new DataBuilder(FIELDS_AB).addTuple("A1", "B1").addTuple("A2", "B2").build();
    Tap<?, ?, ?> tap = new Hfs(OrcFile.source().declaredFields(FIELDS_AB).schemaFromFile().build(), path);

    Plunger.writeData(data).toTap(tap);

    try (OrcReader reader = getOrcReader()) {
      assertThat(reader.hasNext(), is(true));
      List<Object> list = reader.next();
      assertThat(list.size(), is(2));
      assertThat(list.get(0), is((Object) "A1"));
      assertThat(list.get(1), is((Object) "B1"));

      assertThat(reader.hasNext(), is(true));
      list = reader.next();
      assertThat(list.size(), is(2));
      assertThat(list.get(0), is((Object) "A2"));
      assertThat(list.get(1), is((Object) "B2"));

      assertThat(reader.hasNext(), is(false));
    }
  }

  @Test
  public void readTypical() throws IOException {
    try (OrcWriter writer = new OrcWriter.Builder(conf, new Path(path, "part-00000"))
        .addField("a", TypeInfoFactory.stringTypeInfo)
        .addField("b", TypeInfoFactory.stringTypeInfo)
        .build()) {
      writer.addRow("A1", "B1");
      writer.addRow("A2", "B2");
    }

    List<TupleEntry> actual = Plunger.readDataFromTap(
        new Hfs(OrcFile.source().declaredFields(FIELDS_AB).schemaFromFile().build(), path)).asTupleEntryList();
    List<TupleEntry> expected = new DataBuilder(FIELDS_AB)
        .addTuple("A1", "B1")
        .addTuple("A2", "B2")
        .build()
        .asTupleEntryList();

    assertThat(actual, is(tupleEntryList(expected)));
  }

  @Test
  public void readColumnProjectionA() throws IOException {
    try (OrcWriter writer = new OrcWriter.Builder(conf, new Path(path, "part-00000"))
        .addField("a", TypeInfoFactory.stringTypeInfo)
        .addField("b", TypeInfoFactory.stringTypeInfo)
        .build()) {
      writer.addRow("A1", "B1");
      writer.addRow("A2", "B2");
    }

    List<TupleEntry> actual = Plunger.readDataFromTap(
        new Hfs(OrcFile.source().declaredFields(FIELD_A).schemaFromFile().build(), path)).asTupleEntryList();
    List<TupleEntry> expected = new DataBuilder(FIELD_A).addTuple("A1").addTuple("A2").build().asTupleEntryList();
    assertThat(actual, is(tupleEntryList(expected)));
  }

  @Test
  public void readColumnProjectionB() throws IOException {
    OrcWriter.Builder builder = new OrcWriter.Builder(conf, new Path(path, "part-00000")).addField("a",
        TypeInfoFactory.stringTypeInfo).addField("b", TypeInfoFactory.stringTypeInfo);
    try (OrcWriter writer = builder.build()) {
      writer.addRow("A1", "B1");
      writer.addRow("A2", "B2");
    }

    List<TupleEntry> actual = Plunger.readDataFromTap(
        new Hfs(OrcFile.source().declaredFields(FIELD_B).schemaFromFile().build(), path)).asTupleEntryList();
    List<TupleEntry> expected = new DataBuilder(FIELD_B).addTuple("B1").addTuple("B2").build().asTupleEntryList();
    assertThat(actual, is(tupleEntryList(expected)));
  }

  @Test
  public void readWriteInFlowMR1() throws IOException {
    try (OrcWriter writer = new OrcWriter.Builder(conf, new Path(path, "part-00000"))
        .addField("a", TypeInfoFactory.stringTypeInfo)
        .addField("b", TypeInfoFactory.stringTypeInfo)
        .build()) {
      writer.addRow("A1", "B1");
      writer.addRow("A2", "B2");
    }

    String output = new File(temporaryFolder.getRoot(), "output").getCanonicalPath();

    Pipe pipe = new Pipe(UUID.randomUUID().toString());
    FlowDef flowDef = FlowDef
        .flowDef()
        .setName(UUID.randomUUID().toString())
        .addSource(pipe, new Hfs(OrcFile.source().declaredFields(FIELDS_AB).schemaFromFile().build(), path))
        .addTailSink(pipe, new Hfs(OrcFile.sink().schema(FIELDS_AB).build(), output));

    Flow<?> flow = new Hadoop2MR1FlowConnector(HadoopUtil.createProperties(conf)).connect(flowDef);
    flow.complete();
    flow.cleanup();

    try (OrcReader reader = new OrcReader(conf, new Path(output, "part-00000"))) {
      assertThat(reader.hasNext(), is(true));
      List<Object> list = reader.next();
      assertThat(list.size(), is(2));
      assertThat(list.get(0), is((Object) "A1"));
      assertThat(list.get(1), is((Object) "B1"));

      assertThat(reader.hasNext(), is(true));
      list = reader.next();
      assertThat(list.size(), is(2));
      assertThat(list.get(0), is((Object) "A2"));
      assertThat(list.get(1), is((Object) "B2"));

      assertThat(reader.hasNext(), is(false));
    }
  }

  @Test
  public void readWriteInFlowTez() throws IOException {
    try (OrcWriter writer = new OrcWriter.Builder(conf, new Path(path, "part-00000"))
        .addField("a", TypeInfoFactory.stringTypeInfo)
        .addField("b", TypeInfoFactory.stringTypeInfo)
        .build()) {
      writer.addRow("A1", "B1");
      writer.addRow("A2", "B2");
    }

    String output = new File(temporaryFolder.getRoot(), "output").getCanonicalPath();

    Pipe pipe = new Pipe(UUID.randomUUID().toString());
    FlowDef flowDef = FlowDef
        .flowDef()
        .setName(UUID.randomUUID().toString())
        .addSource(pipe, new Hfs(OrcFile.source().declaredFields(FIELDS_AB).schemaFromFile().build(), path))
        .addTailSink(pipe, new Hfs(OrcFile.sink().schema(FIELDS_AB).build(), output));

    Flow<?> flow = new Hadoop2TezFlowConnector(HadoopUtil.createProperties(conf)).connect(flowDef);
    flow.complete();
    flow.cleanup();

    try (OrcReader reader = new OrcReader(conf, new Path(output, "part-v000-o000-00000"))) {
      assertThat(reader.hasNext(), is(true));
      List<Object> list = reader.next();
      assertThat(list.size(), is(2));
      assertThat(list.get(0), is((Object) "A1"));
      assertThat(list.get(1), is((Object) "B1"));

      assertThat(reader.hasNext(), is(true));
      list = reader.next();
      assertThat(list.size(), is(2));
      assertThat(list.get(0), is((Object) "A2"));
      assertThat(list.get(1), is((Object) "B2"));

      assertThat(reader.hasNext(), is(false));
    }
  }

  @Test
  public void readMissing() throws IOException {
    try (OrcWriter writer = new OrcWriter.Builder(conf, new Path(path, "part-00000")).addField("a",
        TypeInfoFactory.stringTypeInfo).build()) {
      writer.addRow("A1");
      writer.addRow("A2");
    }

    List<TupleEntry> actual = Plunger.readDataFromTap(
        new Hfs(OrcFile.source().declaredFields(FIELDS_AB).schemaFromFile().build(), path)).asTupleEntryList();
    List<TupleEntry> expected = new DataBuilder(FIELDS_AB)
        .addTuple("A1", null)
        .addTuple("A2", null)
        .build()
        .asTupleEntryList();
    assertThat(actual, is(tupleEntryList(expected)));
  }

  @Test
  public void writeViaTypeInfo() throws IOException {
    StructTypeInfo typeInfo = new StructTypeInfoBuilder()
        .add("a", TypeInfoFactory.stringTypeInfo)
        .add("b", TypeInfoFactory.getListTypeInfo(TypeInfoFactory.stringTypeInfo))
        .add("c", TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo))
        .add("d", TypeInfoFactory.getCharTypeInfo(1))
        .add("e", TypeInfoFactory.getVarcharTypeInfo(1))
        .add("f", TypeInfoFactory.getDecimalTypeInfo(2, 1))
        .build();

    Fields fields = SchemaFactory.newFields(typeInfo);

    Map<String, String> map = new HashMap<>();
    map.put("C1", "C1");
    Data data = new DataBuilder(fields)
        .addTuple("A1", Arrays.asList("B1"), map, "x", "y", new BigDecimal("1.234"))
        .build();
    Tap<?, ?, ?> tap = new Hfs(OrcFile.source().columns(typeInfo).schemaFromFile().build(), path);

    Plunger.writeData(data).toTap(tap);

    try (OrcReader reader = getOrcReader()) {
      assertThat(reader.hasNext(), is(true));
      List<Object> list = reader.next();
      assertThat(list.size(), is(6));
      assertThat(list.get(0), is((Object) "A1"));
      assertThat(list.get(1), is((Object) Arrays.asList("B1")));
      assertThat(list.get(2), is((Object) map));
      assertThat(((HiveChar) list.get(3)).getValue(), is((Object) "x"));
      assertThat(((HiveVarchar) list.get(4)).getValue(), is((Object) "y"));
      assertThat(((HiveDecimal) list.get(5)).bigDecimalValue(), is((Object) new BigDecimal("1.2")));

      assertThat(reader.hasNext(), is(false));
    }
  }

  @Test
  public void readFromTransactionalTable() throws Exception {
    Fields fields = new Fields(names("id", "msg"), types(int.class, String.class));
    List<TupleEntry> actual = Plunger.readDataFromTap(
        new Hfs(OrcFile.source().declaredFields(fields).schemaFromFile().build(),
            "src/test/data/test_table/continent=Asia/country=India")).asTupleEntryList();
    List<TupleEntry> expected = new DataBuilder(fields)
        .addTuple(2, "UPDATED: Streaming to welcome")
        .addTuple(3, "updated")
        .build()
        .asTupleEntryList();
    assertThat(actual, is(tupleEntryList(expected)));
  }

  @Test
  public void readFromTransactionalTableWithRowId() throws Exception {
    // "ROW__ID" is a magic value
    Fields fields = new Fields(names("ROW__ID", "id", "msg"), types(RecordIdentifier.class, int.class, String.class));
    List<TupleEntry> actual = Plunger.readDataFromTap(
        new Hfs(OrcFile.source().declaredFields(fields).schemaFromFile().build(),
            "src/test/data/test_table/continent=Asia/country=India")).asTupleEntryList();

    List<TupleEntry> expected = new DataBuilder(fields)
        .addTuple(new RecordIdentifier(1, 0, 1), 2, "UPDATED: Streaming to welcome")
        .addTuple(new RecordIdentifier(7, 0, 0), 3, "updated")
        .build()
        .asTupleEntryList();
    assertThat(actual, is(tupleEntryList(expected)));
  }

  @Test
  public void readStringPredicatePushdown() throws IOException {
    TypeInfo typeInfo = TypeInfoFactory.stringTypeInfo;

    try (OrcWriter writer = getOrcWriter(typeInfo)) {
      writer.addRow("hello");
      writer.addRow("world");
    }

    StructTypeInfo structTypeInfo = new StructTypeInfoBuilder().add("a", TypeInfoFactory.stringTypeInfo).build();

    SearchArgument searchArgument = SearchArgumentFactory.newBuilder().startAnd().equals("a", "hello").end().build();

    OrcFile orcFile = OrcFile.source().columns(structTypeInfo).schemaFromFile().searchArgument(searchArgument).build();
    Tap<?, ?, ?> tap = new Hfs(orcFile, path);

    List<Tuple> list = Plunger.readDataFromTap(tap).asTupleList();

    assertThat(list.size(), is(1));
    assertThat(list.get(0).getObject(0), is((Object) "hello"));
  }

  @Test
  public void readDatePredicatePushdown() throws IOException {
    TypeInfo typeInfo = TypeInfoFactory.dateTypeInfo;

    Date date1 = Date.valueOf("1970-01-01");
    Date date2 = Date.valueOf("1970-01-02");

    try (OrcWriter writer = getOrcWriter(typeInfo)) {
      writer.addRow(date1);
      writer.addRow(date2);
    }

    StructTypeInfo structTypeInfo = new StructTypeInfoBuilder().add("a", TypeInfoFactory.dateTypeInfo).build();

    SearchArgument searchArgument = SearchArgumentFactory
        .newBuilder()
        .startAnd()
        .equals("a", new DateWritable(date1))
        .end()
        .build();

    OrcFile orcFile = OrcFile.source().columns(structTypeInfo).schemaFromFile().searchArgument(searchArgument).build();
    Tap<?, ?, ?> tap = new Hfs(orcFile, path);

    List<Tuple> list = Plunger.readDataFromTap(tap).asTupleList();

    assertThat(list.size(), is(1));
    assertThat(((Date) list.get(0).getObject(0)).getTime(), is(date1.getTime()));
  }

  @Test
  public void readDecimalPredicatePushdown() throws IOException {
    TypeInfo typeInfo = TypeInfoFactory.getDecimalTypeInfo(2, 1);

    try (OrcWriter writer = getOrcWriter(typeInfo)) {
      writer.addRow(HiveDecimal.create("0.0"));
      writer.addRow(HiveDecimal.create("0.1"));
    }

    StructTypeInfo structTypeInfo = new StructTypeInfoBuilder().add("a", typeInfo).build();

    SearchArgument searchArgument = SearchArgumentFactory
        .newBuilder()
        .startAnd()
        .equals("a", new BigDecimal("0.1"))
        .end()
        .build();

    OrcFile orcFile = OrcFile.source().columns(structTypeInfo).schemaFromFile().searchArgument(searchArgument).build();
    Tap<?, ?, ?> tap = new Hfs(orcFile, path);

    List<Tuple> list = Plunger.readDataFromTap(tap).asTupleList();

    assertThat(list.size(), is(1));
    assertThat(list.get(0).getObject(0), is((Object) new BigDecimal("0.1")));
  }

  @Test
  public void readCharPredicatePushdown() throws IOException {
    TypeInfo typeInfo = TypeInfoFactory.getCharTypeInfo(3);

    try (OrcWriter writer = getOrcWriter(typeInfo)) {
      writer.addRow(new HiveChar("foo", 3));
      writer.addRow(new HiveChar("bar", 3));
    }

    StructTypeInfo structTypeInfo = new StructTypeInfoBuilder().add("a", typeInfo).build();

    SearchArgument searchArgument = SearchArgumentFactory
        .newBuilder()
        .startAnd()
        .equals("a", new HiveChar("foo", 5))
        .end()
        .build();

    OrcFile orcFile = OrcFile.source().columns(structTypeInfo).schemaFromFile().searchArgument(searchArgument).build();
    Tap<?, ?, ?> tap = new Hfs(orcFile, path);

    List<Tuple> list = Plunger.readDataFromTap(tap).asTupleList();

    assertThat(list.size(), is(1));
    assertThat(list.get(0).getObject(0), is((Object) "foo"));
  }

  @Test(expected = TupleException.class)
  public void readIncorrectType() throws IOException {
    TypeInfo typeInfo = TypeInfoFactory.stringTypeInfo;

    try (OrcWriter writer = getOrcWriter(typeInfo)) {
      writer.addRow("hello");
    }

    Fields intField = new Fields("A", int.class);
    OrcFile orcFile = OrcFile.source().declaredFields(intField).schema(intField).build();
    Tap<?, ?, ?> tap = new Hfs(orcFile, path);

    Plunger.readDataFromTap(tap);
  }

  @Test(expected = TupleException.class)
  public void writeIncorrectType() throws IOException {
    Fields intField = new Fields("A", int.class);
    OrcFile orcFile = OrcFile.sink().schema(intField).build();
    Tap<?, ?, ?> tap = new Hfs(orcFile, path);

    Data data = new DataBuilder(FIELD_A).addTuple("hello").build();

    Plunger.writeData(data).toTap(tap);
  }

  @Test
  public void writeUnionString() throws IOException {
    List<Object> values = new ArrayList<>();
    values.add("hello");
    values.add(null);

    write(TypeInfoFactory.getUnionTypeInfo(Arrays.asList((TypeInfo) TypeInfoFactory.stringTypeInfo)), values);

    try (OrcReader reader = getOrcReader()) {
      assertThat(reader.hasNext(), is(true));
      assertThat(reader.next().get(0), is((Object) "hello"));

      assertThat(reader.hasNext(), is(true));
      assertThat(reader.next().get(0), is(nullValue()));

      assertThat(reader.hasNext(), is(false));
    }
  }

  @Test
  public void readUnionString() throws IOException {
    TypeInfo typeInfo = TypeInfoFactory.getUnionTypeInfo(Arrays.asList((TypeInfo) TypeInfoFactory.stringTypeInfo));

    try (OrcWriter writer = getOrcWriter(typeInfo)) {
      writer.addRow(new StandardUnion((byte) 0, "hello"));
      writer.addRow((Object) null);
    }

    List<Tuple> list = read(typeInfo);
    assertThat(list.size(), is(2));
    assertThat(list.get(0).getObject(0), is((Object) "hello"));
    assertThat(list.get(1).getObject(0), is(nullValue()));
  }

}
