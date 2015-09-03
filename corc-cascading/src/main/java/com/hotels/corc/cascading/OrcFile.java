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

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.hotels.corc.ConverterFactory;
import com.hotels.corc.Corc;
import com.hotels.corc.mapred.CorcInputFormat;
import com.hotels.corc.mapred.CorcOutputFormat;

/**
 * OrcFile provides direct support for the <a
 * href="https://cwiki.apache.org/confluence/display/Hive/LanguageManual+ORC">Optimized Row Columnar (ORC) file
 * format</a>.
 * <p/>
 * The constructor expects that the provided {@link Fields} instance contains {@link Type Types} so that it may create
 * the appropriate {@link SettableStructObjectInspector} when writing.
 * <p/>
 * Lowercase names are enforced when reading from and writing to the {@link OrcStruct} with each column's
 * {@link StructField}. The {@link Fields} names are left as declared (case sensitive). However, case insensitive name
 * clashes are checked for and an {@link IllegalArgumentException} is thrown if detected.
 * <p/>
 * When reading, if a column name is declared that does not exist in the ORC file, the incoming {@link Tuple} stream
 * will contain {@code null} for that column. This enables support for evolving source data where, for example, older
 * data does not contain newer columns.
 * <p/>
 * Column projection is also supported. If the provided {@link Fields} or {@link StructTypeInfo} instance contains only
 * a subset of the columns that exist in the ORC file, only those columns will actually be read. This will minimize the
 * amount of required read IO and improve performance.
 */
@SuppressWarnings("rawtypes")
public class OrcFile extends Scheme<JobConf, RecordReader, OutputCollector, Corc, Corc> {

  /**
   * Returns an object to assist with building an {@link OrcFile} source.
   */
  public static SourceBuilder source() {
    return new SourceBuilder();
  }

  /**
   * Returns an object to assist with building an {@link OrcFile} sink.
   */
  public static SinkBuilder sink() {
    return new SinkBuilder();
  }

  private static final long serialVersionUID = 1L;
  static final String ROW_ID_NAME = VirtualColumn.ROWID.getName();
  /* Double underscore is intentional - matches Hive nomenclature. */
  public static final Fields ROW__ID = new Fields(ROW_ID_NAME, RecordIdentifier.class);
  public static final boolean IGNORE_ROW_ID = false;

  private final StructTypeInfo typeInfo;
  private final StructTypeInfo schemaTypeInfo;
  private final String searchArgumentKryo;
  private final SchemeType type;
  private final ConverterFactory converterFactory;

  /** Source constructor - see {@link SourceBuilder} for example usage. */
  public OrcFile(StructTypeInfo typeInfo, SearchArgument searchArgument, Fields fields, StructTypeInfo schemaTypeInfo,
      ConverterFactory converterFactory) {
    this(typeInfo, searchArgument, fields, schemaTypeInfo, converterFactory, SchemeType.SOURCE);
  }

  /** Sink constructor - see {@link SinkBuilder} for example usage. */
  public OrcFile(Fields fields, StructTypeInfo schemaTypeInfo, ConverterFactory converterFactory) {
    this(schemaTypeInfo, null, fields, schemaTypeInfo, converterFactory, SchemeType.SINK);
  }

  private OrcFile(StructTypeInfo typeInfo, SearchArgument searchArgument, Fields fields, StructTypeInfo schemaTypeInfo,
      ConverterFactory converterFactory, SchemeType type) {
    super(fields, fields);
    validateNamesUnique(typeInfo.getAllStructFieldNames());
    this.typeInfo = typeInfo;
    this.schemaTypeInfo = schemaTypeInfo;
    searchArgumentKryo = searchArgument == null ? null : searchArgument.toKryo();
    this.converterFactory = converterFactory;
    this.type = type;
  }

  /**
   * For full ORC compatibility, field names should be unique when lowercased.
   */
  private void validateNamesUnique(List<String> names) {
    List<String> seenNames = new ArrayList<>(names.size());
    for (int i = 0; i < names.size(); i++) {
      String lowerCaseName = names.get(i).toLowerCase();
      int index = seenNames.indexOf(lowerCaseName);
      if (index != -1) {
        throw new IllegalArgumentException("Duplicate field: " + lowerCaseName + " found at positions " + i + "and"
            + index + ". Field names are case insensitive and must be unique.");
      }
      seenNames.add(lowerCaseName);
    }
  }

  /**
   * Sets the {@link InputFormat} to {@link CorcInputFormat} and stores the {@link StructTypeInfo} in the
   * {@link JobConf}.
   */
  @Override
  public void sourceConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap,
      JobConf conf) {
    conf.setInputFormat(CorcInputFormat.class);
    CorcInputFormat.setSchemaTypeInfo(conf, schemaTypeInfo);
    CorcInputFormat.setTypeInfo(conf, typeInfo);
    CorcInputFormat.setSearchArgumentKryo(conf, searchArgumentKryo);
    CorcInputFormat.setConverterFactoryClass(conf, converterFactory.getClass().asSubclass(ConverterFactory.class));
  }

  /**
   * Creates an {@link Corc} instance and stores it in the context to be reused for all rows.
   */
  @Override
  public void sourcePrepare(FlowProcess<JobConf> flowProcess, SourceCall<Corc, RecordReader> sourceCall)
      throws IOException {
    sourceCall.setContext((Corc) sourceCall.getInput().createValue());
  }

  /**
   * Populates the {@link Corc} with the next value from the {@link RecordReader}. Then copies the values into the
   * incoming {@link TupleEntry}.
   */
  @Override
  public boolean source(FlowProcess<JobConf> flowProcess, SourceCall<Corc, RecordReader> sourceCall) throws IOException {
    Corc corc = sourceCall.getContext();
    @SuppressWarnings("unchecked")
    boolean next = sourceCall.getInput().next(NullWritable.get(), corc);
    if (!next) {
      return false;
    }
    TupleEntry tupleEntry = sourceCall.getIncomingEntry();
    for (Comparable<?> fieldName : tupleEntry.getFields()) {
      if (ROW_ID_NAME.equals(fieldName)) {
        tupleEntry.setObject(ROW_ID_NAME, corc.getRecordIdentifier());
      } else {
        tupleEntry.setObject(fieldName, corc.get(fieldName.toString()));
      }
    }
    return true;
  }

  /**
   * Sets the {@link OutputFormat} to {@link CorcOutputFormat}, sets the key and values to {@link NullWritable} and
   * {@link Corc} respectively.
   */
  @Override
  public void sinkConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap,
      JobConf conf) {
    conf.setOutputFormat(CorcOutputFormat.class);
    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputValueClass(Corc.class);
  }

  /**
   * Creates an {@link Corc} instance and stores it in the context to be reused for all rows.
   */
  @Override
  public void sinkPrepare(FlowProcess<JobConf> flowProcess, SinkCall<Corc, OutputCollector> sinkCall)
      throws IOException {
    sinkCall.setContext(new Corc(typeInfo, converterFactory));
  }

  /**
   * Copies the values from the outgoing {@link TupleEntry} to the {@link Corc}.
   */
  @SuppressWarnings("unchecked")
  @Override
  public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Corc, OutputCollector> sinkCall) throws IOException {
    Corc corc = sinkCall.getContext();
    TupleEntry tupleEntry = sinkCall.getOutgoingEntry();
    for (Comparable<?> fieldName : tupleEntry.getFields()) {
      corc.set(fieldName.toString(), tupleEntry.getObject(fieldName));
    }
    sinkCall.getOutput().collect(null, corc);
  }

  @Override
  public boolean isSymmetrical() {
    return false;
  }

  @Override
  public boolean isSource() {
    return SchemeType.SOURCE == type;
  }

  @Override
  public boolean isSink() {
    return SchemeType.SINK == type;
  }

  /**
   * Builds an {@link OrcFile} instance that can be used to read.
   */
  public static final class SourceBuilder {

    private StructTypeInfo columnTypeInfo;
    private Fields fields;
    private StructTypeInfo schemaTypeInfo;
    private SearchArgument searchArgument;
    private boolean includeRowId;
    private boolean schemaFromFile;
    private ConverterFactory converterFactory = new CascadingConverterFactory();

    SourceBuilder() {
    }

    /**
     * Set the declared {@link Fields} for this scheme.
     * <p/>
     * If the fields supplied represent a subset of the schema and the {@link #columns(StructTypeInfo)} or
     * {@link #columns(String)} column projection will occur.
     * <p/>
     * Only Hive primitive types with equivalent Java types are supported:
     * <ul>
     * <li>{@link java.util.String}</li>
     * <li>{@link java.util.Boolean}</li>
     * <li>{@link java.util.Byte}</li>
     * <li>{@link java.util.Short}</li>
     * <li>{@link java.util.Integer}</li>
     * <li>{@link java.util.Long}</li>
     * <li>{@link java.util.Float}</li>
     * <li>{@link java.util.Double}</li>
     * <li>{@link java.sql.Timestamp}</li>
     * <li>{@link java.sql.Date}</li>
     * <li>{@link Byte#TYPE byte[]}</li>
     * </ul>
     * <p/>
     * {@code CHAR}, {@code VARCHAR}, {@code DECIMAL}, {@code STRUCT}, {@code ARRAY}, {@code MAP} and {@code UNIONTYPE}
     * are not supported as sub-types cannot be inferred.
     * <p/>
     * Should the respective schema {@link TypeInfo} for a column not be coercible to the type declared in the specified
     * {@link Fields} instance an {@link IllegalStateException} will be thrown when building the {@link OrcFile}.
     */
    public SourceBuilder declaredFields(Fields fields) {
      checkExisting(this.fields, "declared fields");
      checkFields(fields);
      this.fields = fields;
      return this;
    }

    /**
     * Specify a subset of columns to read. All Hive types are supported. If the {@link #declaredFields(Fields)} option
     * has not been specified the declared {@link Fields} for this scheme will be derived from the supplied column
     * {@link StructTypeInfo}.
     * <p/>
     * Should the respective schema {@link TypeInfo} for a column not be coercible to the type declared in the specified
     * {@link StructTypeInfo} instance an {@link IllegalStateException} will be thrown when building the {@link OrcFile}.
     */
    public SourceBuilder columns(StructTypeInfo typeInfo) {
      checkExisting(columnTypeInfo, "columns");
      checkNotNull(typeInfo, "typeInfo");
      columnTypeInfo = typeInfo;
      return this;
    }

    /**
     * Specify a subset of columns to read. All Hive types are supported. If the {@link #declaredFields(Fields)} option
     * has not been specified the declared {@link Fields} for this scheme will be derived from the supplied column
     * {@link StructTypeInfo}.
     * <p/>
     * Should the respective schema {@link TypeInfo} for a column not be coercible to the type declared in the specified
     * {@link StructTypeInfo} instance an {@link IllegalStateException} will be thrown when building the {@link OrcFile}.
     * <p/>
     * <b>Useful for reads only.</b>
     */
    public SourceBuilder columns(String typeInfoString) {
      checkExisting(columnTypeInfo, "columns");
      columnTypeInfo = (StructTypeInfo) TypeInfoUtils.getTypeInfoFromTypeString(typeInfoString);
      return this;
    }

    /**
     * Include the {@link VirtualColumn#ROWID ROW__ID} virtual column when reading ORC files that back a transactional
     * Hive table. The column will be prepended to the record's {@link Fields}.
     */
    public SourceBuilder prependRowId() {
      if (includeRowId) {
        throw new IllegalStateException("You've already select the prependRowId option.");
      }
      includeRowId = true;
      return this;
    }

    /**
     * Specify the schema of the file. All Hive types are supported. If the {@link #declaredFields(Fields)} and
     * {@link #columns(StructTypeInfo) columns(...)} options were not specified, the declared {@link Fields} will be
     * derived from the supplied schema {@link StructTypeInfo}.
     * <p/>
     * Only Hive primitive types with equivalent Java types are supported:
     * <ul>
     * <li>{@link java.util.String}</li>
     * <li>{@link java.util.Boolean}</li>
     * <li>{@link java.util.Byte}</li>
     * <li>{@link java.util.Short}</li>
     * <li>{@link java.util.Integer}</li>
     * <li>{@link java.util.Long}</li>
     * <li>{@link java.util.Float}</li>
     * <li>{@link java.util.Double}</li>
     * <li>{@link java.sql.Timestamp}</li>
     * <li>{@link java.sql.Date}</li>
     * <li>{@link Byte#TYPE byte[]}</li>
     * </ul>
     * <p/>
     * {@code CHAR}, {@code VARCHAR}, {@code DECIMAL}, {@code STRUCT}, {@code ARRAY}, {@code MAP} and {@code UNIONTYPE}
     * are not supported as sub-types cannot be inferred.
     */
    public SourceBuilder schema(Fields fields) {
      checkExistingSchema();
      checkFields(fields);
      schemaTypeInfo = SchemaFactory.newStructTypeInfo(fields);
      return this;
    }

    /**
     * Specify the schema of the file. All Hive types are supported. If the {@link #declaredFields(Fields)} and
     * {@link #columns(StructTypeInfo) columns(...)} options were not specified, the declared {@link Fields} will be
     * derived from the supplied schema {@link StructTypeInfo}.
     * <p/>
     * Should the respective schema {@link TypeInfo} for a column be different from the {@link TypeInfo} in the actual
     * ORC File then a {@link ClassCastException} will be thrown when reading the records.
     */
    public SourceBuilder schema(StructTypeInfo typeInfo) {
      checkExistingSchema();
      checkNotNull(typeInfo, "typeInfo");
      schemaTypeInfo = typeInfo;
      return this;
    }

    /**
     * Specify the schema of the file. All Hive types are supported. If the {@link #declaredFields(Fields)} and
     * {@link #columns(StructTypeInfo) columns(...)} options were not specified, the declared {@link Fields} will be
     * derived from the supplied schema {@link StructTypeInfo}.
     * <p/>
     * Should the respective schema {@link TypeInfo} for a column be different from the {@link TypeInfo} in the actual
     * ORC File then a {@link ClassCastException} will be thrown when reading the records.
     */
    public SourceBuilder schema(String typeInfoString) {
      checkExistingSchema();
      schemaTypeInfo = (StructTypeInfo) TypeInfoUtils.getTypeInfoFromTypeString(typeInfoString);
      return this;
    }

    /**
     * Read the schema from the underlying ORC files - you must declare the {@link #declaredFields(Fields) fields}
     * and/or the {@link #columns(StructTypeInfo)} you wish to read if you select this option. Do not use this option
     * for ACID data sets.
     */
    public SourceBuilder schemaFromFile() {
      checkExistingSchema();
      schemaFromFile = true;
      return this;
    }

    /**
     * Apply predicate pushdown by passing in a valid {@link SearchArgument}.
     */
    public SourceBuilder searchArgument(SearchArgument searchArgument) {
      checkExisting(this.searchArgument, "a search argument");
      checkNotNull(searchArgument, "searchArgument");
      this.searchArgument = searchArgument;
      return this;
    }

    /**
     * Provide a {@link ConverterFactory} if you want to use different java types than the defaults.
     */
    public SourceBuilder converterFactory(ConverterFactory converterFactory) {
      checkNotNull(converterFactory, "converterFactory");
      this.converterFactory = converterFactory;
      return this;
    }

    public OrcFile build() {
      if (!schemaFromFile && schemaTypeInfo == null) {
        throw new IllegalStateException("You must set a source for the file schema.");
      }
      if (columnTypeInfo == null) {
        if (fields != null) {
          columnTypeInfo = SchemaFactory.newStructTypeInfo(fields);
        } else if (schemaTypeInfo != null) {
          columnTypeInfo = schemaTypeInfo;
        } else {
          throw new IllegalStateException("You must declare at least the fields, columns, or schema for this source.");
        }
      }
      if (fields == null) {
        fields = SchemaFactory.newFields(columnTypeInfo);
      }
      Fields sourceFields;
      if (includeRowId) {
        sourceFields = ROW__ID.append(fields);
      } else {
        sourceFields = fields;
      }
      return new OrcFile(columnTypeInfo, searchArgument, sourceFields, schemaTypeInfo, converterFactory);
    }

    private void checkExistingSchema() {
      if (schemaFromFile) {
        throw new IllegalStateException("You've already specified that the schema be read from a file.");
      }
      if (schemaTypeInfo != null) {
        throw new IllegalStateException("You've already specified a schema: " + schemaTypeInfo);
      }
    }

    private static void checkExisting(Object value, String name) {
      if (value != null) {
        throw new IllegalStateException("You've already specified " + name + ": " + value);
      }
    }
  }

  /**
   * Builds an {@link OrcFile} instance that can be used to write.
   */
  public static final class SinkBuilder {

    private Fields fields;
    private StructTypeInfo schemaTypeInfo;
    private ConverterFactory converterFactory = new CascadingConverterFactory();

    SinkBuilder() {
    }

    /**
     * Specify the schema of the file.
     * <p/>
     * Care must be taken when deciding how best to specify the types of {@link Fields} as there is not a one-to-one
     * bidirectional mapping between Cascading types and Hive types. The {@link TypeInfo TypeInfo} is able to represent
     * richer, more complex types. Consider your ORC schema and the mappings to {@link Fields} types carefully.
     * {@link TypeInfo} variants of {@link #schema(TypeInfo) schema(...)} are provided to allow finer control if needed.
     * <p/>
     * Only Hive primitive types with equivalent Java types are supported:
     * <ul>
     * <li>{@link java.util.String}</li>
     * <li>{@link java.util.Boolean}</li>
     * <li>{@link java.util.Byte}</li>
     * <li>{@link java.util.Short}</li>
     * <li>{@link java.util.Integer}</li>
     * <li>{@link java.util.Long}</li>
     * <li>{@link java.util.Float}</li>
     * <li>{@link java.util.Double}</li>
     * <li>{@link java.sql.Timestamp}</li>
     * <li>{@link java.sql.Date}</li>
     * <li>{@link Byte#TYPE byte[]}</li>
     * </ul>
     * <p/>
     * {@code CHAR}, {@code VARCHAR}, {@code DECIMAL}, {@code STRUCT}, {@code ARRAY}, {@code MAP} and {@code UNIONTYPE}
     * are not supported as sub-types cannot be inferred.
     */
    public SinkBuilder schema(Fields fields) {
      checkForExistingSchema();
      checkFields(fields);
      this.fields = fields;
      return this;
    }

    /**
     * Specify the schema of the file. All Hive types are supported.
     * <p/>
     * Should the {@link Type} of the respective outgoing {@link Fields field} not be coercible to the {@link TypeInfo}
     * in the specified {@link StructTypeInfo} instance, a {@link ClassCastException} will be thrown when writing the
     * record.
     */
    public SinkBuilder schema(StructTypeInfo typeInfo) {
      checkForExistingSchema();
      checkNotNull(typeInfo, "typeInfo");
      schemaTypeInfo = typeInfo;
      return this;
    }

    /**
     * Specify the schema of the file. All Hive types are supported.
     * <p/>
     * Should the {@link Type} of the respective outgoing {@link Fields field} not be coercible to the {@link TypeInfo}
     * in the specified {@link StructTypeInfo} instance, a {@link ClassCastException} will be thrown when writing the
     * record.
     */
    public SinkBuilder schema(String typeInfoString) {
      checkForExistingSchema();
      schemaTypeInfo = (StructTypeInfo) TypeInfoUtils.getTypeInfoFromTypeString(typeInfoString);
      return this;
    }

    /**
     * Provide a {@link ConverterFactory} if you want to use different java types than the defaults.
     */
    public SinkBuilder converterFactory(ConverterFactory converterFactory) {
      checkNotNull(converterFactory, "converterFactory");
      this.converterFactory = converterFactory;
      return this;
    }

    public OrcFile build() {
      if (fields == null && schemaTypeInfo == null) {
        throw new IllegalArgumentException("You must declare at least the sink fields or the file schema.");
      }
      if (fields == null) {
        fields = SchemaFactory.newFields(schemaTypeInfo);
      } else if (schemaTypeInfo == null) {
        schemaTypeInfo = SchemaFactory.newStructTypeInfo(fields);
      }
      return new OrcFile(fields, schemaTypeInfo, converterFactory);
    }

    private void checkForExistingSchema() {
      if (schemaTypeInfo != null) {
        throw new IllegalStateException("You've already specified the schema: " + schemaTypeInfo);
      }
      if (fields != null) {
        throw new IllegalStateException("You've already specified the schema using fields: " + fields);
      }
    }

  }

  private static final void checkFields(Fields fields) {
    if (fields == null || fields.size() <= 0) {
      throw new IllegalArgumentException("Invalid fields: " + fields);
    }
  }

  private static final void checkNotNull(Object value, String name) {
    if (value == null) {
      throw new IllegalArgumentException(name + " == null");
    }
  }

  private static enum SchemeType {
    SOURCE,
    SINK;
  }

}
