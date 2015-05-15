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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * Convenience builder.
 */
public class StructTypeInfoBuilder {

  private final List<String> names = new ArrayList<>();
  private final List<TypeInfo> typeInfos = new ArrayList<>();

  public StructTypeInfoBuilder add(String name, TypeInfo typeInfo) {
    if (name == null || name.trim().length() == 0) {
      throw new IllegalArgumentException("name must not be null or empty");
    }
    for (String existingName : names) {
      if (existingName.equalsIgnoreCase(name)) {
        throw new IllegalArgumentException("duplicate name: " + name);
      }
    }
    if (typeInfo == null) {
      throw new IllegalArgumentException("typeInfo must not be null");
    }
    names.add(name);
    typeInfos.add(typeInfo);
    return this;
  }

  public StructTypeInfo build() {
    return (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(names, typeInfos);
  }

}
