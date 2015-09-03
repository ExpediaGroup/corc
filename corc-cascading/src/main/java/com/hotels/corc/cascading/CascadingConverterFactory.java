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
package com.hotels.corc.cascading;

import java.math.BigDecimal;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

import com.hotels.corc.BaseConverter;
import com.hotels.corc.Converter;
import com.hotels.corc.DefaultConverterFactory;
import com.hotels.corc.UnexpectedTypeException;

public class CascadingConverterFactory extends DefaultConverterFactory {

  private static final long serialVersionUID = 1L;

  @Override
  public Converter newConverter(ObjectInspector inspector) {
    switch (inspector.getCategory()) {
    case PRIMITIVE:
      switch (((PrimitiveObjectInspector) inspector).getPrimitiveCategory()) {
      case CHAR:
        return new CharConverter();
      case VARCHAR:
        return new VarcharConverter();
      case DECIMAL:
        return new DecimalConverter();
      default:
        return super.newConverter(inspector);
      }
    default:
      return super.newConverter(inspector);
    }
  }

  public static class CharConverter extends BaseConverter {

    @Override
    protected Object toWritableObjectInternal(Object value) throws UnexpectedTypeException {
      if (value instanceof String) {
        value = new HiveChar((String) value, -1);
      }
      return new HiveCharWritable((HiveChar) value);
    }

    @Override
    protected Object toJavaObjectInternal(Object value) throws UnexpectedTypeException {
      return ((HiveCharWritable) value).getHiveChar().getValue();
    }

  }

  public static class VarcharConverter extends BaseConverter {

    @Override
    protected Object toWritableObjectInternal(Object value) throws UnexpectedTypeException {
      if (value instanceof String) {
        value = new HiveVarchar((String) value, -1);
      }
      return new HiveVarcharWritable((HiveVarchar) value);
    }

    @Override
    protected Object toJavaObjectInternal(Object value) throws UnexpectedTypeException {
      return ((HiveVarcharWritable) value).getHiveVarchar().getValue();
    }

  }

  public static class DecimalConverter extends BaseConverter {

    @Override
    protected Object toWritableObjectInternal(Object value) throws UnexpectedTypeException {
      if (value instanceof String) {
        value = HiveDecimal.create((String) value);
      } else if (value instanceof BigDecimal) {
        value = HiveDecimal.create((BigDecimal) value);
      }
      return new HiveDecimalWritable((HiveDecimal) value);
    }

    @Override
    protected Object toJavaObjectInternal(Object value) throws UnexpectedTypeException {
      return ((HiveDecimalWritable) value).getHiveDecimal().bigDecimalValue();
    }

  }

}
