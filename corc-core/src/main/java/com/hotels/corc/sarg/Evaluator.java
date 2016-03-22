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
package com.hotels.corc.sarg;

import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.TruthValue;

import com.hotels.corc.Corc;

abstract class Evaluator<T extends Comparable<T>> {

  private final String fieldName;

  protected Evaluator(String fieldName) {
    this.fieldName = fieldName;
  }

  @SuppressWarnings("unchecked")
  TruthValue evaluate(Corc corc) {
    return evaluate((T) corc.getWritable(fieldName));
  }

  protected abstract TruthValue evaluate(T value);

}
