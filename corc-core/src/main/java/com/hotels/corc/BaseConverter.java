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