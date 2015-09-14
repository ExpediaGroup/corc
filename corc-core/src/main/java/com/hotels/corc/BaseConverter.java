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

public abstract class BaseConverter implements Converter {

  @Override
  public Object toWritableObject(Object value) throws UnexpectedTypeException {
    if (value == null) {
      return null;
    }
    try {
      return toWritableObjectInternal(value);
    } catch (ClassCastException e) {
      throw new UnexpectedTypeException(value);
    }
  }

  protected abstract Object toWritableObjectInternal(Object value) throws UnexpectedTypeException;

  @Override
  public Object toJavaObject(Object value) throws UnexpectedTypeException {
    if (value == null) {
      return null;
    }
    try {
      return toJavaObjectInternal(value);
    } catch (ClassCastException e) {
      throw new UnexpectedTypeException(value);
    }
  }

  protected abstract Object toJavaObjectInternal(Object value) throws UnexpectedTypeException;

}