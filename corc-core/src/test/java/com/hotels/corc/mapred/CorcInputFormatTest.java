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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.corc.Corc;
import com.hotels.corc.DefaultConverterFactory;
import com.hotels.corc.Filter;
import com.hotels.corc.StructTypeInfoBuilder;
import com.hotels.corc.sarg.SearchArgumentFilter;
import com.hotels.corc.test.OrcWriter;

@RunWith(MockitoJUnitRunner.class)
public class CorcInputFormatTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Mock
  private Reporter reporter;

  private final JobConf conf = new JobConf();
  private final CorcInputFormat inputFormat = new CorcInputFormat();

  private File file;
  private Path path;
  private FileSplit split;

  @Before
  public void before() throws IOException {
    file = new File(temporaryFolder.getRoot(), "part-00000");
    path = new Path(file.getCanonicalPath());

    try (OrcWriter writer = new OrcWriter.Builder(conf, path)
        .addField("a", TypeInfoFactory.stringTypeInfo)
        .addField("b", TypeInfoFactory.stringTypeInfo)
        .build()) {
      writer.addRow("A1", "B1");
    }
    split = new FileSplit(path, 0L, file.length(), (String[]) null);
  }

  @Test(expected = IOException.class)
  public void notAFileSplit() throws IOException {
    InputSplit split = mock(InputSplit.class);

    inputFormat.getRecordReader(split, conf, reporter);
  }

  @Test
  public void readColumnProjection() throws IOException {
    StructTypeInfo typeInfo = new StructTypeInfoBuilder().add("a", TypeInfoFactory.stringTypeInfo).build();
    CorcInputFormat.setTypeInfo(conf, typeInfo);
    CorcInputFormat.setConverterFactoryClass(conf, DefaultConverterFactory.class);

    RecordReader<NullWritable, Corc> reader = inputFormat.getRecordReader(split, conf, reporter);

    Corc corc = reader.createValue();

    reader.next(NullWritable.get(), corc);
    assertThat(corc.get("a"), is((Object) "A1"));
    assertThat(corc.get("b"), is(nullValue()));
    reader.close();
  }

  @Test
  public void getSplits() throws IOException {
    conf.set("mapred.input.dir", temporaryFolder.getRoot().getCanonicalPath());
    InputSplit[] splits = inputFormat.getSplits(conf, 1);

    assertThat(splits.length, is(1));
    FileSplit actual = (FileSplit) splits[0];
    assertThat(actual.getPath().toUri().getRawPath(), is(path.toUri().getRawPath()));
    assertThat(actual.getStart(), is(0L));
    assertThat(actual.getLength(), is(file.length()));
  }

  @Test
  public void readFullyReadSchemaFromSplit() throws IOException {
    StructTypeInfo typeInfo = new StructTypeInfoBuilder()
        .add("a", TypeInfoFactory.stringTypeInfo)
        .add("b", TypeInfoFactory.stringTypeInfo)
        .build();
    CorcInputFormat.setTypeInfo(conf, typeInfo);
    CorcInputFormat.setConverterFactoryClass(conf, DefaultConverterFactory.class);

    RecordReader<NullWritable, Corc> reader = inputFormat.getRecordReader(split, conf, reporter);

    Corc corc = reader.createValue();

    reader.next(NullWritable.get(), corc);
    assertThat(corc.get("a"), is((Object) "A1"));
    assertThat(corc.get("b"), is((Object) "B1"));
    reader.close();
  }

  @Test
  public void readFullyDeclaredSchema() throws IOException {
    StructTypeInfo typeInfo = new StructTypeInfoBuilder()
        .add("a", TypeInfoFactory.stringTypeInfo)
        .add("b", TypeInfoFactory.stringTypeInfo)
        .build();
    CorcInputFormat.setTypeInfo(conf, typeInfo);
    CorcInputFormat.setSchemaTypeInfo(conf, typeInfo);
    CorcInputFormat.setConverterFactoryClass(conf, DefaultConverterFactory.class);

    RecordReader<NullWritable, Corc> reader = inputFormat.getRecordReader(split, conf, reporter);

    Corc corc = reader.createValue();

    reader.next(NullWritable.get(), corc);
    assertThat(corc.get("a"), is((Object) "A1"));
    assertThat(corc.get("b"), is((Object) "B1"));
    reader.close();
  }

  @Test
  public void setInputTypeInfo() {
    StructTypeInfo typeInfo = new StructTypeInfoBuilder()
        .add("a", TypeInfoFactory.stringTypeInfo)
        .add("b", TypeInfoFactory.stringTypeInfo)
        .build();
    CorcInputFormat.setTypeInfo(conf, typeInfo);

    assertThat(conf.get(CorcInputFormat.INPUT_TYPE_INFO), is("struct<a:string,b:string>"));
  }

  @Test
  public void getInputTypeInfo() {
    conf.set(CorcInputFormat.INPUT_TYPE_INFO, "struct<a:string,b:string>");

    StructTypeInfo typeInfo = new StructTypeInfoBuilder()
        .add("a", TypeInfoFactory.stringTypeInfo)
        .add("b", TypeInfoFactory.stringTypeInfo)
        .build();
    assertThat(CorcInputFormat.getTypeInfo(conf), is(typeInfo));
  }

  @Test
  public void setSchemaTypeInfo() {
    StructTypeInfo typeInfo = new StructTypeInfoBuilder()
        .add("a", TypeInfoFactory.stringTypeInfo)
        .add("b", TypeInfoFactory.stringTypeInfo)
        .build();
    CorcInputFormat.setSchemaTypeInfo(conf, typeInfo);

    assertThat(conf.get(CorcInputFormat.SCHEMA_TYPE_INFO), is("struct<a:string,b:string>"));
  }

  @Test
  public void setSchemaTypeInfoNull() {
    CorcInputFormat.setSchemaTypeInfo(conf, null);

    assertThat(conf.get(CorcInputFormat.SCHEMA_TYPE_INFO), is(nullValue()));
  }

  @Test
  public void getSchemaTypeInfo() {
    conf.set(CorcInputFormat.SCHEMA_TYPE_INFO, "struct<a:string,b:string>");

    StructTypeInfo typeInfo = new StructTypeInfoBuilder()
        .add("a", TypeInfoFactory.stringTypeInfo)
        .add("b", TypeInfoFactory.stringTypeInfo)
        .build();
    assertThat(CorcInputFormat.getSchemaTypeInfo(conf), is(typeInfo));
  }

  @Test
  public void getSchemaTypeInfoNull() {
    assertThat(CorcInputFormat.getSchemaTypeInfo(conf), is(nullValue()));
  }

  @Test
  public void getSchemaTypeInfoEmpty() {
    conf.set(CorcInputFormat.SCHEMA_TYPE_INFO, "");

    assertThat(CorcInputFormat.getSchemaTypeInfo(conf), is(nullValue()));
  }

  @Test
  public void setInputReadColumnProjection() {
    StructTypeInfo typeInfo = new StructTypeInfoBuilder()
        .add("a", TypeInfoFactory.stringTypeInfo)
        .add("b", TypeInfoFactory.longTypeInfo)
        .build();

    conf.set(CorcInputFormat.INPUT_TYPE_INFO, "struct<a:string>");

    CorcInputFormat.setReadColumns(conf, typeInfo);

    assertThat(conf.getBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, true), is(false));
    assertThat(conf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR), is("a"));
    assertThat(conf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR), is("0"));
  }

  @Test
  public void setInputReadColumnsAll() {
    StructTypeInfo typeInfo = new StructTypeInfoBuilder()
        .add("a", TypeInfoFactory.stringTypeInfo)
        .add("b", TypeInfoFactory.longTypeInfo)
        .build();

    conf.set(CorcInputFormat.INPUT_TYPE_INFO, "struct<a:string,b:bigint>");

    CorcInputFormat.setReadColumns(conf, typeInfo);

    assertThat(conf.getBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, true), is(false));
    assertThat(conf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR), is("a,b"));
    assertThat(conf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR), is("0,1"));
  }

  @Test
  public void setInputReadColumnsMissing() {
    StructTypeInfo typeInfo = new StructTypeInfoBuilder()
        .add("a", TypeInfoFactory.stringTypeInfo)
        .add("b", TypeInfoFactory.longTypeInfo)
        .build();

    conf.set(CorcInputFormat.INPUT_TYPE_INFO, "struct<a:string,b:bigint,c:string>");

    CorcInputFormat.setReadColumns(conf, typeInfo);

    assertThat(conf.getBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, true), is(false));
    assertThat(conf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR), is("0,1"));
  }

  @Test(expected = IllegalStateException.class)
  public void setInputReadColumnsAllMissing() {
    StructTypeInfo typeInfo = new StructTypeInfoBuilder()
        .add("a", TypeInfoFactory.stringTypeInfo)
        .add("b", TypeInfoFactory.longTypeInfo)
        .build();

    conf.set(CorcInputFormat.INPUT_TYPE_INFO, "struct<_col0:string,_col1:bigint>");

    CorcInputFormat.setReadColumns(conf, typeInfo);
  }

  @Test(expected = IllegalStateException.class)
  public void setInputReadColumnsDifferentTypes() {
    StructTypeInfo typeInfo = new StructTypeInfoBuilder().add("a", TypeInfoFactory.stringTypeInfo).build();

    conf.set(CorcInputFormat.INPUT_TYPE_INFO, "struct<a:bigint>");

    CorcInputFormat.setReadColumns(conf, typeInfo);
  }

  @Test
  public void setSearchArgument() {
    SearchArgument searchArgument = SearchArgumentFactory.newBuilder().startAnd().equals("a", "b").end().build();
    CorcInputFormat.setSearchArgument(conf, searchArgument);

    String kryo = conf.get(CorcInputFormat.SEARCH_ARGUMENT);

    assertThat(kryo, is(searchArgument.toKryo()));
  }

  @Test
  public void setSearchArgumentKryo() {
    CorcInputFormat.setSearchArgumentKryo(conf, null);

    String kryo = conf.get(CorcInputFormat.SEARCH_ARGUMENT);

    assertThat(kryo, is(nullValue()));
  }

  @Test
  public void setSearchArgumentNull() {
    CorcInputFormat.setSearchArgument(conf, null);

    String kryo = conf.get(CorcInputFormat.SEARCH_ARGUMENT);

    assertThat(kryo, is(nullValue()));
  }

  @Test
  public void getSearchArgument() {
    SearchArgument searchArgument = SearchArgumentFactory.newBuilder().startAnd().equals("a", "b").end().build();
    conf.set(CorcInputFormat.SEARCH_ARGUMENT, searchArgument.toKryo());

    String kryo = CorcInputFormat.getSearchArgument(conf).toKryo();

    assertThat(kryo, is(searchArgument.toKryo()));
  }

  @Test
  public void getFilterDisabledSearchArgument() {
    StructTypeInfo typeInfo = new StructTypeInfoBuilder().add("a", TypeInfoFactory.stringTypeInfo).build();
    SearchArgument searchArgument = SearchArgumentFactory.newBuilder().startAnd().equals("a", "b").end().build();
    conf.set(CorcInputFormat.SEARCH_ARGUMENT, searchArgument.toKryo());
    conf.setBoolean(CorcInputFormat.ENABLE_ROW_LEVEL_SEARCH_ARGUMENT, false);

    assertThat(CorcInputFormat.getFilter(conf, typeInfo), is(Filter.ACCEPT));
  }

  @Test
  public void getFilterNoSearchArgument() {
    StructTypeInfo typeInfo = new StructTypeInfoBuilder().add("a", TypeInfoFactory.stringTypeInfo).build();
    assertThat(CorcInputFormat.getFilter(conf, typeInfo), is(Filter.ACCEPT));
  }

  @Test
  public void getFilterWithSearchArgument() {
    StructTypeInfo typeInfo = new StructTypeInfoBuilder().add("a", TypeInfoFactory.stringTypeInfo).build();
    SearchArgument searchArgument = SearchArgumentFactory.newBuilder().startAnd().equals("a", "b").end().build();
    conf.set(CorcInputFormat.SEARCH_ARGUMENT, searchArgument.toKryo());
    assertThat(CorcInputFormat.getFilter(conf, typeInfo), instanceOf(SearchArgumentFilter.class));
  }

  @Test
  public void getSearchArgumentNull() {
    SearchArgument searchArgument = CorcInputFormat.getSearchArgument(conf);

    assertThat(searchArgument, is(nullValue()));
  }

  @Test(expected = RuntimeException.class)
  public void converterFactoryNull() {
    CorcInputFormat.getConverterFactory(conf);
  }

  @Test
  public void converterFactory() {
    CorcInputFormat.setConverterFactoryClass(conf, DefaultConverterFactory.class);

    assertThat(conf.get(CorcInputFormat.CONVERTER_FACTORY), is("com.hotels.corc.DefaultConverterFactory"));
  }

}
