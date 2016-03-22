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

import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;

import cascading.tuple.Fields;

/**
 * A {@link org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory} that uses {@link Fields}. Extracts the column name
 * from the field and checks that the values passed conform to the type declared in the field.
 * <p/>
 * Side note: according to the {@link org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf.Operator ORC Javadoc}, if you wish
 * to apply operators not provided on the {@link org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory ORC
 * SearchArgumentFactory} (such as GT, GTE, NE, ...), you must employ the {@link Builder#startNot() not} operator with
 * the respective inverse operator. We've built this into the builder for GREATER_THAN and GREATER_THAN_EQUALS.
 */
public final class SearchArgumentFactory {

  /** See {@link org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory#newBuilder()}. */
  public static SearchArgumentFactory.Builder newBuilder() {
    org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder internalBuilder = org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory
        .newBuilder();
    return new Builder(internalBuilder);
  }

  private SearchArgumentFactory() {
  }

  /** See {@link org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder}. */
  public static final class Builder {

    private final org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder internalBuilder;

    // Package private for testing
    Builder(org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder internalBuilder) {
      this.internalBuilder = internalBuilder;
    }

    /** See {@link org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder#between(String, Object, Object)}. */
    public Builder between(Fields fields, Object lower, Object upper) {
      checkFields(fields);
      checkValueTypes(fields, lower, upper);
      internalBuilder.between(toName(fields), lower, upper);
      return this;
    }

    /** See {@link org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder#end()}. */
    public Builder end() {
      internalBuilder.end();
      return this;
    }

    /** See {@link org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder#equals(String, Object)}. */
    public Builder equals(Fields fields, Object literal) {
      checkFields(fields);
      checkValueTypes(fields, literal);
      internalBuilder.equals(toName(fields), literal);
      return this;
    }

    /** See {@link org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder#in(String, Object...)}. */
    public Builder in(Fields fields, Object... literals) {
      checkFields(fields);
      if (literals != null && literals.length > 0) {
        checkValueTypes(fields, literals);
        // we just check types, leave the null/length validation to the original implementation.
      }
      internalBuilder.in(toName(fields), literals);
      return this;
    }

    /** See {@link org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder#isNull(String)}. */
    public Builder isNull(Fields fields) {
      checkFields(fields);
      checkValueTypes(fields, new Object[] { null });
      internalBuilder.isNull(toName(fields));
      return this;
    }

    /** See {@link org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder#lessThan(String, Object)}. */
    public Builder lessThan(Fields fields, Object literal) {
      checkFields(fields);
      checkValueTypes(fields, literal);
      internalBuilder.lessThan(toName(fields), literal);
      return this;
    }

    /** See {@link org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder#lessThanEquals(String, Object)}. */
    public Builder lessThanEquals(Fields fields, Object literal) {
      checkFields(fields);
      checkValueTypes(fields, literal);
      internalBuilder.lessThanEquals(toName(fields), literal);
      return this;
    }

    /**
     * Comprises: {@link org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder#startNot() startNot()}.
     * {@link org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder#lessThanEquals(String, Object)
     * lessThanEquals(String, Object)}.{@link org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder#end() end()}.
     */
    public Builder greaterThan(Fields fields, Object literal) {
      checkFields(fields);
      checkValueTypes(fields, literal);
      internalBuilder.startNot().lessThanEquals(toName(fields), literal).end();
      return this;
    }

    /**
     * Comprises: {@link org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder#startNot() startNot()}.
     * {@link org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder#lessThan(String, Object) lessThan(String,
     * Object)}.{@link org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder#end() end()}.
     */
    public Builder greaterThanEquals(Fields fields, Object literal) {
      checkFields(fields);
      checkValueTypes(fields, literal);
      internalBuilder.startNot().lessThan(toName(fields), literal).end();
      return this;
    }
    
    /** See {@link org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder#nullSafeEquals(String, Object)}. */
    public Builder nullSafeEquals(Fields fields, Object literal) {
      checkFields(fields);
      checkValueTypes(fields, literal);
      internalBuilder.nullSafeEquals(toName(fields), literal);
      return this;
    }

    /** See {@link org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder#startAnd()}. */
    public Builder startAnd() {
      internalBuilder.startAnd();
      return this;
    }

    /** See {@link org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder#startNot()}. */
    public Builder startNot() {
      internalBuilder.startNot();
      return this;
    }

    /** See {@link org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder#startOr()}. */
    public Builder startOr() {
      internalBuilder.startOr();
      return this;
    }

    /** See {@link org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder#build()}. */
    public SearchArgument build() {
      return internalBuilder.build();
    }

    // Package private for testing
    static String toName(Fields fields) {
      return fields.get(0).toString().toLowerCase();
    }

    // Package private for testing
    static void checkFields(Fields fields) {
      if (fields == null) {
        throw new IllegalArgumentException("fields == null");
      }
      if (fields.size() != 1) {
        throw new IllegalArgumentException("You may only specify a single field: " + fields);
      }
      if (fields.getTypes() == null) {
        throw new IllegalArgumentException("Fields must declare types: " + fields);
      }
      if (fields.getTypeClass(0) == null) {
        throw new IllegalArgumentException("Fields must declare types: " + fields);
      }
    }

    // Package private for testing
    static void checkValueTypes(Fields fields, Object... values) {
      Class<?> typeClass = fields.getTypeClass(0);
      String fieldName = fields.get(0).toString();
      for (Object value : values) {
        if (value == null && typeClass.isPrimitive()) {
          throw new IllegalArgumentException("Field '" + fieldName + "' is primitive, cannot be null.");
        } else if (value != null && !typeClass.isAssignableFrom(value.getClass())) {
          throw new IllegalArgumentException("Field '" + fieldName + "' value '" + value
              + "' is not of the correct type. Was " + value.getClass() + " expected derivative of " + typeClass);
        }
      }
    }

  }

}
