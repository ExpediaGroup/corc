       O~~~   O~~    O~ O~~~   O~~~
     O~~    O~~  O~~  O~~    O~~   
    O~~    O~~    O~~ O~~   O~~    
     O~~    O~~  O~~  O~~    O~~   
       O~~~   O~~    O~~~      O~~~

Use corc to read and write data in the [Optimized Row Columnar (ORC)](http://orc.apache.org/) file format in your Cascading applications. The reading of [ACID](http://orc.apache.org/docs/acid.html) datasets is also supported.

## Status ⚠️

This project is no longer in active development.

# Start using
You can obtain **corc** from Maven Central :

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.hotels/corc-cascading/badge.svg?subject=com.hotels:corc-cascading)](https://maven-badges.herokuapp.com/maven-central/com.hotels/corc-cascading) ![GitHub license](https://img.shields.io/github/license/ExpediaGroup/corc.svg)

## Cascading Dependencies

Corc has been built and tested against Cascading 3.3.0.

## Hive Dependencies

Corc is built with Hive 2.3.4. Several dependencies will need to be included when using Corc:

    <dependency>
      <groupId>org.apache.hive</groupId>
      <artifactId>hive-exec</artifactId>
      <version>2.3.4</version>
      <classifier>core</classifier>
    </dependency>
    <dependency>
      <groupId>org.apache.hive</groupId>
      <artifactId>hive-serde</artifactId>
      <version>2.3.4</version>
    </dependency>
    <dependency>
      <groupId>com.esotericsoftware.kryo</groupId>
      <artifactId>kryo</artifactId>
      <version>2.22</version>
    </dependency>
    <dependency>
      <groupId>com.google.protobuf</groupId>
      <artifactId>protobuf-java</artifactId>
      <version>2.5.0</version>
    </dependency>

# Overview
## [Supported types](http://orc.apache.org/docs/types.html)

<table>
  <tr><th>Hive</th><th>Cascading/Java</th></tr>
  <tr><td>STRING</td><td>String</td></tr>
  <tr><td>BOOLEAN</td><td>Boolean</td></tr>
  <tr><td>TINYINT</td><td>Byte</td></tr>
  <tr><td>SMALLINT</td><td>Short</td></tr>
  <tr><td>INT</td><td>Integer</td></tr>
  <tr><td>BIGINT</td><td>Long</td></tr>
  <tr><td>FLOAT</td><td>Float</td></tr>
  <tr><td>DOUBLE</td><td>Double</td></tr>
  <tr><td>TIMESTAMP</td><td>java.sql.Timestamp</td></tr>
  <tr><td>DATE</td><td>java.sql.Date</td></tr>
  <tr><td>BINARY</td><td>byte[]</td></tr>
  <tr><td>CHAR</td><td>String (HiveChar)</td></tr>
  <tr><td>VARCHAR</td><td>String (HiveVarchar)</td></tr>
  <tr><td>DECIMAL</td><td>BigDecimal (HiveDecimal)</td></tr>
  <tr><td>ARRAY</td><td>List&lt;Object&gt;</td></tr>
  <tr><td>MAP</td><td>Map&lt;Object, Object&gt;</td></tr>
  <tr><td>STRUCT</td><td>List&lt;Object&gt;</td></tr>
  <tr><td>UNIONTYPE</td><td>Sub-type</td></tr>
</table>


## Constructing an `OrcFile` instance

`OrcFile` provides two public constructors; one for sourcing and one for sinking. However, these are provided to be more flexible for others who may wish to extend the class. It is advised to construct an instance via the `SourceBuilder` and `SinkBuilder` classes.

### SourceBuilder

Create a builder:

    SourceBuilder builder = OrcFile.source();

Specify the fields that should be read. If the declared schema is a subset of the complete schema, then column projection will occur:

    builder.declaredFields(fields);
    // or
    builder.columns(structTypeInfo);
    // or
    builder.columns(structTypeInfoString);

Specify the complete schema of the underlying ORC Files. This is only required for reading ORC Files that back a transactional Hive table. The default behaviour should be to obtain the schema from the ORC Files being read:

    builder.schemaFromFile();
    // or
    builder.schema(fields);
    // or
    builder.schema(structTypeInfo);
    // or
    builder.schema(structTypeInfoString);

ORC Files support [predicate pushdown](http://orc.apache.org/docs/indexes.html). This allows whole row groups to be skipped if they do not contain any rows that match the given `SearchArgument`:

    Fields message = new Fields("message", String.class);
    SearchArgument searchArgument = SearchArgumentFactory.newBuilder()
        .startAnd()
        .equals(message, "hello")
        .end()
        .build();

    builder.searchArgument(searchArgument);

When passing objects to the `SearchArgument.Builder`, care should be taken to choose the correct type:

<table>
  <tr><th>Hive</th><th>Java</th></tr>
  <tr><td>STRING</td><td>String</td></tr>
  <tr><td>BOOLEAN</td><td>Boolean</td></tr>
  <tr><td>TINYINT</td><td>Byte</td></tr>
  <tr><td>SMALLINT</td><td>Short</td></tr>
  <tr><td>INT</td><td>Integer</td></tr>
  <tr><td>BIGINT</td><td>Long</td></tr>
  <tr><td>FLOAT</td><td>Float</td></tr>
  <tr><td>DOUBLE</td><td>Double</td></tr>
  <tr><td>TIMESTAMP</td><td>java.sql.Timestamp</td></tr>
  <tr><td>DATE</td><td>org.apache.hadoop.hive.serde2.io.DateWritable</td></tr>
  <tr><td>CHAR</td><td>String (HiveChar)</td></tr>
  <tr><td>VARCHAR</td><td>String (HiveVarchar)</td></tr>
  <tr><td>DECIMAL</td><td>BigDecimal</td></tr>
</table>

When reading ORC Files that back a transactional Hive table, include the `VirtualColumn#ROWID` ("ROW__ID") virtual column. The column will be prepended to the record's `Fields`:

    builder.prependRowId();

Finally, build the `OrcFile`:

    OrcFile orcFile = builder.build();

### SinkBuilder

    OrcFile orcFile = OrcFile.sink()
        .schema(schema)
        .build();

The `schema` parameter can be one of `Fields`, `StructTypeInfo` or the `String` representation of the `StructTypeInfo`. When providing a `Fields` instance, care must be taken when deciding how best to specify the types as there is no one-to-one bidirectional mapping between Cascading types and Hive types. The `TypeInfo` is able to represent richer, more complex types. Consider your ORC File schema and the mappings to `Fields` types carefully.

### Constructing a `StructTypeInfo` instance

    List<String> names = new ArrayList<>();
    names.add("col0");
    names.add("col1");

    List<TypeInfo> typeInfos = new ArrayList<>();
    typeInfos.add(TypeInfoFactory.stringTypeInfo);
    typeInfos.add(TypeInfoFactory.longTypeInfo);

    StructTypeInfo structTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(names, typeInfos);

or...

    String typeString = "struct<col0:string,col1:bigint>";

    StructTypeInfo structTypeInfo = (StructTypeInfo) TypeInfoUtils.getTypeInfoFromTypeString(typeString);

or, via the convenience builder...

    StructTypeInfo structTypeInfo = new StructTypeInfoBuilder()
        .add("col0", TypeInfoFactory.stringTypeInfo)
        .add("col1", TypeInfoFactory.longTypeInfo)
        .build();

## Reading transactional Hive tables
Corc also supports the reading of [ACID datasets](http://orc.apache.org/docs/acid.html) that underpin [transactional Hive tables](https://cwiki.apache.org/confluence/display/Hive/Hive+Transactions). However, for this to work effectively with an active Hive table you must provide your own lock management. We intend to make this functionality available in the [cascading-hive](https://github.com/ExpediaGroup/cascading-hive/tree/acid) project. When reading the data you may optionally include the virtual `RecordIdentifer` column, also known as the `ROW__ID` column, with one of the following approaches:

1. Add a field named '`ROW__ID`' to your `Fields` definition. This must be of type `org.apache.hadoop.hive.ql.io.RecordIdentifier`. For convenience you can use the constant `OrcFile#ROW__ID` with some fields arithmetic: `Fields myFields = Fields.join(OrcFile.ROW__ID, myFields);`.
2. Use the `OrcFile.source().prependRowId()` option. Be sure to exclude the `RecordIdentifer` column from your `typeInfo` instance. The `ROW__ID` field will be added to your tuple stream automatically.

## Usage
`OrcFile` can be used with `Hfs`, just like `TextDelimited`.

    OrcFile orcFile = ...
    String path = ...
    Hfs hfs = new Hfs(orcFile, path);

# Credits

Created by [Dave Maughan](https://github.com/nahguam) & [Elliot West](https://github.com/teabot), with thanks to: [Patrick Duin](https://github.com/patduin), [James Grant](https://github.com/noddy76) & [Adrian Woodhead](https://github.com/massdosage).

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2015-2020 Expedia, Inc.
