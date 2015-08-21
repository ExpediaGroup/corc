/**
 * Copyright 2015 Expedia Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.corc;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObject;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * The {@code OrcUnion} class and its associated ObjectInspector are entirely package private. The alternative to this
 * approach is to use {@link TypeInfoUtils#getStandardWritableObjectInspectorFromTypeInfo} for writing. It's preferable
 * to consistently use {@link OrcStruct#createObjectInspector} for both reading and writing and put up with this small
 * (hopefully temporary) workaround for this rarely used type.
 */
final class OrcUnionFactory {

  private static final Constructor<? extends UnionObject> CONSTRUCTOR;
  private static final Method SET_METHOD;

  static {
    try {
      Class<? extends UnionObject> orcUnionClass = Class
          .forName("org.apache.hadoop.hive.ql.io.orc.OrcUnion")
          .asSubclass(UnionObject.class);
      CONSTRUCTOR = orcUnionClass.getDeclaredConstructor();
      CONSTRUCTOR.setAccessible(true);
      SET_METHOD = orcUnionClass.getDeclaredMethod("set", byte.class, Object.class);
      SET_METHOD.setAccessible(true);
    } catch (ClassNotFoundException | NoSuchMethodException | SecurityException e) {
      throw new RuntimeException(e);
    }
  }

  private OrcUnionFactory() {
  }

  static UnionObject newInstance(byte tag, Object value) {
    try {
      UnionObject orcUnion = CONSTRUCTOR.newInstance();
      SET_METHOD.invoke(orcUnion, tag, value);
      return orcUnion;
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

}
