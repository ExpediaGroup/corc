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
package com.hotels.corc.mapred;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcRecordUpdater;
import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.corc.ConverterFactory;
import com.hotels.corc.Corc;
import com.hotels.corc.Filter;
import com.hotels.corc.sarg.SearchArgumentFilter;

/**
 * A wrapper for {@link OrcInputFormat} to expose {@link Corc} as the value type instead of {@link OrcStruct}. This
 * enables column access by name instead of by position.
 */
public class CorcInputFormat implements InputFormat<NullWritable, Corc> {

  private static final Logger LOG = LoggerFactory.getLogger(CorcInputFormat.class);

  static final String CONVERTER_FACTORY = "com.hotels.corc.mapred.converter.factory";
  static final String SCHEMA_TYPE_INFO = "com.hotels.corc.mapred.schema.type.info";
  static final String INPUT_TYPE_INFO = "com.hotels.corc.mapred.input.type.info";
  static final String SEARCH_ARGUMENT = "sarg.pushdown";
  static final int ATOMIC_ROW_COLUMN_ID;
  static final String ATOMIC_ROW_COLUMN_NAME = "row";

  /**
   * If a {@link SearchArgument} is provided, by default it will be applied at the row level as well as at the row group
   * level. Set this configuration option to false to disable row level evaluation.
   */
  public static final String ENABLE_ROW_LEVEL_SEARCH_ARGUMENT = "com.hotels.corc.mapred.input.enable.row.level.search.argument";

  static {
    ATOMIC_ROW_COLUMN_ID = getOrcAtomicRowColumnId();
  }

  /**
   * Gets the ConverterFactory from the configuration
   */
  static ConverterFactory getConverterFactory(Configuration conf) {
    Class<? extends ConverterFactory> converterFactoryClass = conf.getClass(CONVERTER_FACTORY, null,
        ConverterFactory.class);
    if (converterFactoryClass == null) {
      throw new RuntimeException("ConverterFactory class was not set on the configuration");
    }
    LOG.debug("Got input ConverterFactory class from conf: {}", converterFactoryClass);
    return ReflectionUtils.newInstance(converterFactoryClass, conf);
  }

  /**
   * Sets the ConverterFactory class
   */
  public static void setConverterFactoryClass(Configuration conf,
      Class<? extends ConverterFactory> converterFactoryClass) {
    conf.setClass(CONVERTER_FACTORY, converterFactoryClass, ConverterFactory.class);
    LOG.debug("Set input ConverterFactory class on conf: {}", converterFactoryClass);
  }

  /**
   * Gets the StructTypeInfo that declares the columns to be read from the configuration
   */
  static StructTypeInfo getTypeInfo(Configuration conf) {
    StructTypeInfo inputTypeInfo = (StructTypeInfo) TypeInfoUtils.getTypeInfoFromTypeString(conf.get(INPUT_TYPE_INFO));
    LOG.debug("Got input typeInfo from conf: {}", inputTypeInfo);
    return inputTypeInfo;
  }

  /**
   * Sets the StructTypeInfo that declares the columns to be read in the configuration
   */
  public static void setTypeInfo(Configuration conf, StructTypeInfo typeInfo) {
    conf.set(INPUT_TYPE_INFO, typeInfo.getTypeName());
    LOG.debug("Set input typeInfo on conf: {}", typeInfo);
  }

  /**
   * Gets the StructTypeInfo that declares the total schema of the file from the configuration
   */
  static StructTypeInfo getSchemaTypeInfo(Configuration conf) {
    String schemaTypeInfo = conf.get(SCHEMA_TYPE_INFO);
    if (schemaTypeInfo != null && !schemaTypeInfo.isEmpty()) {
      LOG.debug("Got schema typeInfo from conf: {}", schemaTypeInfo);
      return (StructTypeInfo) TypeInfoUtils.getTypeInfoFromTypeString(conf.get(SCHEMA_TYPE_INFO));
    }
    return null;
  }

  /**
   * Sets the StructTypeInfo that declares the total schema of the file in the configuration
   */
  public static void setSchemaTypeInfo(Configuration conf, StructTypeInfo schemaTypeInfo) {
    if (schemaTypeInfo != null) {
      conf.set(SCHEMA_TYPE_INFO, schemaTypeInfo.getTypeName());
      LOG.debug("Set schema typeInfo on conf: {}", schemaTypeInfo);
    }
  }

  /**
   * Sets the SearchArgument predicate pushdown in the configuration
   */
  public static void setSearchArgument(Configuration conf, SearchArgument searchArgument) {
    if (searchArgument != null) {
      setSearchArgumentKryo(conf, searchArgument.toKryo());
    }
  }

  /**
   * Sets the SearchArgument predicate pushdown in the configuration
   */
  public static void setSearchArgumentKryo(Configuration conf, String searchArgumentKryo) {
    if (searchArgumentKryo != null) {
      conf.set(SEARCH_ARGUMENT, searchArgumentKryo);
    }
  }

  static SearchArgument getSearchArgument(Configuration conf) {
    String searchArgumentKryo = conf.get(SEARCH_ARGUMENT);
    if (searchArgumentKryo == null) {
      return null;
    }
    return SearchArgumentFactory.create(searchArgumentKryo);
  }

  /**
   * Sets which fields are to be read from the ORC file
   */
  static void setReadColumns(Configuration conf, StructTypeInfo actualStructTypeInfo) {
    StructTypeInfo readStructTypeInfo = getTypeInfo(conf);

    List<Integer> ids = new ArrayList<>();
    List<String> names = new ArrayList<>();

    List<String> readNames = readStructTypeInfo.getAllStructFieldNames();
    List<String> actualNames = actualStructTypeInfo.getAllStructFieldNames();

    for (int i = 0; i < actualNames.size(); i++) {
      String actualName = actualNames.get(i);
      if (readNames.contains(actualName)) {
        // make sure they are the same type
        TypeInfo actualTypeInfo = actualStructTypeInfo.getStructFieldTypeInfo(actualName);
        TypeInfo readTypeInfo = readStructTypeInfo.getStructFieldTypeInfo(actualName);
        if (!actualTypeInfo.equals(readTypeInfo)) {
          throw new IllegalStateException("readTypeInfo [" + readTypeInfo + "] does not match actualTypeInfo ["
              + actualTypeInfo + "]");
        }
        // mark the column as to-be-read
        ids.add(i);
        names.add(actualName);
      }
      LOG.debug("Set column projection on columns: {} ({})", ids, readNames);
    }
    ColumnProjectionUtils.appendReadColumns(conf, ids, names);
  }

  private final OrcInputFormat orcInputFormat = new OrcInputFormat();

  @Override
  public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {
    return orcInputFormat.getSplits(conf, numSplits);
  }

  static Filter getFilter(Configuration conf, StructTypeInfo typeInfo) {
    if (conf.getBoolean(ENABLE_ROW_LEVEL_SEARCH_ARGUMENT, true)) {
      SearchArgument searchArgument = getSearchArgument(conf);
      if (searchArgument != null) {
        return new SearchArgumentFilter(searchArgument, typeInfo);
      }
    }
    return Filter.ACCEPT;
  }

  @Override
  public RecordReader<NullWritable, Corc> getRecordReader(InputSplit inputSplit, JobConf conf, Reporter reporter)
      throws IOException {
    StructTypeInfo typeInfo = getSchemaTypeInfo(conf);
    if (typeInfo == null) {
      typeInfo = readStructTypeInfoFromSplit(inputSplit, conf);
    }
    setReadColumns(conf, typeInfo);
    RecordReader<NullWritable, OrcStruct> reader = orcInputFormat.getRecordReader(inputSplit, conf, reporter);
    return new CorcRecordReader(typeInfo, reader, getConverterFactory(conf), getFilter(conf, typeInfo));
  }

  private StructTypeInfo readStructTypeInfoFromSplit(InputSplit inputSplit, JobConf conf) throws IOException {
    LOG.debug("Attempting to read schema typeInfo from split: {}", inputSplit);
    StructTypeInfo typeInfo;
    if (inputSplit instanceof FileSplit) {
      Path path = getSplitPath((FileSplit) inputSplit, conf);
      Reader orcReader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
      ObjectInspector inspector = orcReader.getObjectInspector();
      typeInfo = (StructTypeInfo) TypeInfoUtils.getTypeInfoFromObjectInspector(inspector);

      if (isAtomic(orcReader)) {
        LOG.warn("Split is atomic yet schema typeInfo was not provided via {}."
            + " This is not recommended and will fail if you only have deltas!", SCHEMA_TYPE_INFO);
        typeInfo = extractRowStruct(typeInfo);
      }
    } else {
      throw new IOException("Unsupported InputSplit " + inputSplit.getClass().getName());
    }
    LOG.debug("Read schema typeInfo from split: {}", typeInfo);
    return typeInfo;
  }

  /*
   * This is to work around an issue reading from ORC transactional data sets that contain only deltas. These contain
   * synthesised column names that are not usable to us.
   */
  private Path getSplitPath(FileSplit inputSplit, JobConf conf) throws IOException {
    Path path = inputSplit.getPath();
    if (inputSplit instanceof OrcSplit) {
      OrcSplit orcSplit = (OrcSplit) inputSplit;
      List<Long> deltas = orcSplit.getDeltas();
      if (!orcSplit.hasBase() && deltas.size() >= 2) {
        throw new IOException("Cannot read valid StructTypeInfo from delta only file: " + path);
      }
    }
    LOG.debug("Input split path: {}", path);
    return path;
  }

  private boolean isAtomic(Reader orcReader) {
    // Use org.apache.hadoop.hive.ql.io.orc.OrcInputFormat.isOriginal(Reader) from hive-exec:1.1.0
    boolean atomic = orcReader.hasMetadataValue(OrcRecordUpdater.ACID_KEY_INDEX_NAME);
    LOG.debug("Atomic ORCFile: {}", atomic);
    return atomic;
  }

  private StructTypeInfo extractRowStruct(StructTypeInfo typeInfo) {
    List<String> actualNames = typeInfo.getAllStructFieldNames();
    if (actualNames.size() < ATOMIC_ROW_COLUMN_ID + 1) {
      throw new IllegalArgumentException("Too few rows for a transactional table: " + actualNames);
    }
    String rowStructName = actualNames.get(ATOMIC_ROW_COLUMN_ID);
    if (!ATOMIC_ROW_COLUMN_NAME.equalsIgnoreCase(rowStructName)) {
      throw new IllegalArgumentException("Expected row column name '" + ATOMIC_ROW_COLUMN_NAME + "', found: "
          + rowStructName);
    }
    StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo.getStructFieldTypeInfo(rowStructName);
    LOG.debug("Row StructTypeInfo defined as: {}", structTypeInfo);
    return structTypeInfo;
  }

  /* This ugliness can go when the column id can be referenced from a public place. */
  static private int getOrcAtomicRowColumnId() {
    try {
      Field rowField = OrcRecordUpdater.class.getDeclaredField("ROW");
      rowField.setAccessible(true);
      int rowId = (int) rowField.get(null);
      rowField.setAccessible(false);
      return rowId;
    } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
      throw new RuntimeException("Could not obtain OrcRecordUpdater.ROW value.", e);
    }

  }

}
