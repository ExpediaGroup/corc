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

import cascading.tuple.Fields;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

import java.lang.reflect.Type;
import java.sql.Date;
import java.sql.Timestamp;

final class FieldsTypeUtils {

  private FieldsTypeUtils() {
  }

  static PredicateLeaf.Type toType(Fields fields) {
    Type type = fields.getType(0);
    if (type.equals(Double.class)) {
        return PredicateLeaf.Type.FLOAT;
    } else if (type.equals(Long.class)) {
        return PredicateLeaf.Type.LONG;
    } else if (type.equals(Integer.class)) {
        return PredicateLeaf.Type.LONG;
    } else if (type.equals(Date.class)) {
        return PredicateLeaf.Type.DATE;
    } else if (type.equals(String.class)) {
        return PredicateLeaf.Type.STRING;
    } else if (type.equals(HiveChar.class)){
        return PredicateLeaf.Type.STRING;
    } else if (type.equals(HiveVarchar.class)){
        return PredicateLeaf.Type.STRING;
    } else if(type.equals(HiveDecimalWritable.class)) {
        return PredicateLeaf.Type.DECIMAL;
    } else if (type.equals(Timestamp.class)) {
        return PredicateLeaf.Type.TIMESTAMP;
    } else if (type.equals(Boolean.class)) {
        return PredicateLeaf.Type.BOOLEAN;
    }
    throw new IllegalStateException("Can't map Fields.Type to PredicateLeaf.Type:" + fields);
  }
}
