package com.hotels.corc;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.corc.Converter;
import com.hotels.corc.UnexpectedTypeException;
import com.hotels.corc.ValueMarshaller;
import com.hotels.corc.ValueMarshallerImpl;

@RunWith(MockitoJUnitRunner.class)
public class ValueMarshallerImplTest {

  @Mock
  private SettableStructObjectInspector inspector;
  @Mock
  private StructField structField;
  @Mock
  private Converter converter;

  ValueMarshaller marshaller;

  private final String string = "hello";
  private final Text text = new Text(string);

  @Before
  public void before() {
    marshaller = new ValueMarshallerImpl(inspector, structField, converter);
  }

  @Test
  public void getJava() throws UnexpectedTypeException {
    when(inspector.getStructFieldData(null, structField)).thenReturn(text);
    when(converter.toJavaObject(text)).thenReturn(string);

    Object javaObject = marshaller.getJavaObject(null);

    assertThat(javaObject, is((Object) string));

    verify(inspector).getStructFieldData(null, structField);
    verify(converter).toJavaObject(text);
  }

  @Test(expected = UnexpectedTypeException.class)
  public void getJavaException() throws UnexpectedTypeException {
    when(inspector.getStructFieldData(null, structField)).thenReturn(text);
    doThrow(new UnexpectedTypeException()).when(converter).toJavaObject(text);

    marshaller.getJavaObject(null);
  }

  @Test
  public void setWritable() throws UnexpectedTypeException {
    when(converter.toWritableObject(string)).thenReturn(text);

    marshaller.setWritableObject(null, string);

    verify(inspector).setStructFieldData(null, structField, text);
  }

  @Test(expected = UnexpectedTypeException.class)
  public void setWritableException() throws UnexpectedTypeException {
    doThrow(new UnexpectedTypeException()).when(converter).toWritableObject(string);

    marshaller.setWritableObject(null, string);
  }

}
