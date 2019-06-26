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
package com.hotels.corc;

import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

class ValueMarshallerImpl implements ValueMarshaller {

  private final SettableStructObjectInspector inspector;
  private final StructField structField;
  private final Converter converter;

  ValueMarshallerImpl(SettableStructObjectInspector inspector, StructField structField, Converter converter) {
    this.inspector = inspector;
    this.structField = structField;
    this.converter = converter;
  }

  @Override
  public Object getJavaObject(OrcStruct struct) throws UnexpectedTypeException {
    Object writable = inspector.getStructFieldData(struct, structField);
    try {
      return converter.toJavaObject(writable);
    } catch (UnexpectedTypeException e) {
      throw new UnexpectedTypeException(writable, structField.getFieldName(), e);
    }
  }

  @Override
  public Object getWritableObject(OrcStruct struct) {
    return inspector.getStructFieldData(struct, structField);
  }

  @Override
  public void setWritableObject(OrcStruct struct, Object javaObject) throws UnexpectedTypeException {
    Object writable;
    try {
      writable = converter.toWritableObject(javaObject);
    } catch (UnexpectedTypeException e) {
      throw new UnexpectedTypeException(javaObject, structField.getFieldName(), e);
    }
    inspector.setStructFieldData(struct, structField, writable);
  }
}