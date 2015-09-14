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
package com.hotels.corc;

import org.apache.hadoop.hive.ql.io.orc.OrcStruct;

interface ValueMarshaller {

  static final ValueMarshaller NULL = new ValueMarshaller() {

    @Override
    public Object getJavaObject(OrcStruct struct) throws UnexpectedTypeException {
      return null;
    }

    @Override
    public Object getWritableObject(OrcStruct struct) {
      return null;
    }

    @Override
    public void setWritableObject(OrcStruct struct, Object javaObject) throws UnexpectedTypeException {
      // do nothing
    }
  };

  Object getJavaObject(OrcStruct struct) throws UnexpectedTypeException;

  Object getWritableObject(OrcStruct struct);

  void setWritableObject(OrcStruct struct, Object javaObject) throws UnexpectedTypeException;

}