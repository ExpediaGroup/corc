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

import java.io.IOException;

/** Thrown when the type of a value to be marshalled does not match the type declared for the field. */
public class UnexpectedTypeException extends IOException {

  private static final long serialVersionUID = 1L;

  public UnexpectedTypeException(Object value, String fieldName, Throwable cause) {
    super(String.format("Unexpected value %s of type %s for field %s.", value, value.getClass(), fieldName), cause);
  }

  public UnexpectedTypeException(Object value) {
    super(String.format("Unexpected value %s of type %s.", value, value.getClass()));
  }

  public UnexpectedTypeException() {
    // Intended to ease mocking by corc users.
    super();
  }

}
