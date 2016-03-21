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

import static com.hotels.corc.cascading.OrcFile.ROW_ID_NAME;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import cascading.tuple.Fields;

final class SchemaFactory {

  private SchemaFactory() {
  }

  private static final Map<TypeInfo, Class<?>> PRIMITIVES;

  static {
    Map<TypeInfo, Class<?>> primitives = new HashMap<>();
    primitives.put(TypeInfoFactory.stringTypeInfo, String.class);
    primitives.put(TypeInfoFactory.booleanTypeInfo, Boolean.class);
    primitives.put(TypeInfoFactory.byteTypeInfo, Byte.class);
    primitives.put(TypeInfoFactory.shortTypeInfo, Short.class);
    primitives.put(TypeInfoFactory.intTypeInfo, Integer.class);
    primitives.put(TypeInfoFactory.longTypeInfo, Long.class);
    primitives.put(TypeInfoFactory.floatTypeInfo, Float.class);
    primitives.put(TypeInfoFactory.doubleTypeInfo, Double.class);
    primitives.put(TypeInfoFactory.timestampTypeInfo, Timestamp.class);
    primitives.put(TypeInfoFactory.dateTypeInfo, Date.class);
    primitives.put(TypeInfoFactory.binaryTypeInfo, byte[].class);
    PRIMITIVES = Collections.unmodifiableMap(primitives);
  }

  static Fields newFields(StructTypeInfo structTypeInfo) {
    List<String> existingNames = structTypeInfo.getAllStructFieldNames();
    List<String> namesList = new ArrayList<>(existingNames.size());
    namesList.addAll(existingNames);
    String[] names = namesList.toArray(new String[namesList.size()]);

    List<TypeInfo> typeInfos = structTypeInfo.getAllStructFieldTypeInfos();
    Class<?>[] types = new Class[typeInfos.size()];
    for (int i = 0; i < types.length; i++) {
      Class<?> type = PRIMITIVES.get(typeInfos.get(i));
      if (type == null) {
        type = Object.class;
      }
      types[i] = type;
    }

    return new Fields(names, types);
  }

  static StructTypeInfo newStructTypeInfo(Fields fields) {
    List<String> names = new ArrayList<>();
    List<TypeInfo> typeInfos = new ArrayList<>();

    for (int i = 0; i < fields.size(); i++) {
      String name = fields.get(i).toString();
      if (ROW_ID_NAME.equals(name)) {
        if (!fields.getTypeClass(i).equals(RecordIdentifier.class)) {
          throw new IllegalArgumentException(ROW_ID_NAME + " column is not of type "
              + RecordIdentifier.class.getSimpleName() + ". Found type: " + fields.getTypeClass(i));
        }
        continue;
      }
      names.add(name.toLowerCase());
      Class<?> type = fields.getTypeClass(i);
      if (type == null) {
        throw new IllegalArgumentException("Missing type information for field: " + name);
      }

      TypeInfo typeInfo = getTypeInfoFromClass(type);
      typeInfos.add(typeInfo);
    }

    return (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(names, typeInfos);
  }

  private static TypeInfo getTypeInfoFromClass(Class<?> type) {
    TypeInfo typeInfo = null;
    if (BigDecimal.class.equals(type)) {
      typeInfo = TypeInfoFactory.decimalTypeInfo;
    } else {
      typeInfo = TypeInfoFactory.getPrimitiveTypeInfoFromJavaPrimitive(type);
    }
    return typeInfo;
  }

}
