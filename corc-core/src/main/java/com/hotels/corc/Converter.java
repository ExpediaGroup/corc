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

public interface Converter {

  /**
   * Converts the provided Java value to the equivalent {@link Writable} type. An {@link UnexpectedTypeException} will
   * be thrown if the provided value is not of the expected type.
   */
  Object toWritableObject(Object value) throws UnexpectedTypeException;

  /**
   * Converts the provided {@link Writable} value to the equivalent Java type. An {@link UnexpectedTypeException} will
   * be thrown if the provided value is not of the expected type.
   */
  Object toJavaObject(Object value) throws UnexpectedTypeException;

}