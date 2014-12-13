package com.theladders.storm;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;

public abstract class AbstractAnnotatedBoltTest
{

  @Mock
  private OutputFieldsDeclarer   outputFieldsDeclarer;

  @Captor
  private ArgumentCaptor<Fields> fieldsArgumentCaptor;

  @Mock
  private OutputCollector        outputCollector;

  @Captor
  private ArgumentCaptor<Values> valuesArgumentCaptor;

  protected String[]             inputFields;
  protected Object[]             inputValues;

  protected Fields               declaredFields;

  protected Tuple                tuple;

  @Before
  public void setup()
  {
    MockitoAnnotations.initMocks(this);
  }

  public void whenRunning(Object targetBolt)
  {
    List list = Arrays.asList(inputValues);
    GeneralTopologyContext context = mock(GeneralTopologyContext.class);
    when(context.getComponentOutputFields(anyString(), anyString())).thenReturn(new Fields(inputFields));
    tuple = new TupleImpl(context, list, 1, "streamId");

    AnnotatedBolt annotatedBolt = new AnnotatedBolt(targetBolt);
    annotatedBolt.prepare(new HashMap(), mock(TopologyContext.class), outputCollector);
    annotatedBolt.declareOutputFields(outputFieldsDeclarer);
    annotatedBolt.execute(tuple);
    annotatedBolt.cleanup();
  }

  protected void givenInputFields(String... fields)
  {
    inputFields = fields;
  }

  protected void givenInputValues(Object... values)
  {
    inputValues = values;
  }

  protected void thenTheOutputFieldsAre(String... values)
  {
    verify(outputFieldsDeclarer).declare(fieldsArgumentCaptor.capture());
    Fields fields = fieldsArgumentCaptor.getValue();
    assertEquals(Arrays.asList("field1", "field2"), fields.toList());
  }

  private void thenTheOutputValuesAre(Object... values)
  {
    List<Object> returnedValues = valuesArgumentCaptor.getValue();
    assertEquals(values.length, returnedValues.size());
    for (int i = 0; i < values.length; i++)
    {
      Object expectedValue = values[i];
      Object actualValue = returnedValues.get(i);
      assertEquals(expectedValue, actualValue);
    }
  }

  protected void thenTheOutputValuesAre(String streamId,
                                        Object... values)
  {
    verify(outputCollector).emit(eq(streamId), eq(tuple), valuesArgumentCaptor.capture());
    List<Object> returnedValues = valuesArgumentCaptor.getValue();
    assertEquals(values.length, returnedValues.size());
    for (int i = 0; i < values.length; i++)
    {
      Object expectedValue = values[i];
      Object actualValue = returnedValues.get(i);
      assertEquals(expectedValue, actualValue);
    }
  }

  protected void thenFailureWasReportedFor(Class<? extends Throwable> expectedErrorClass)
  {
    verify(outputCollector).reportError(any(expectedErrorClass));
  }

  protected void thenFailureWasNotReported()
  {
    verify(outputCollector, never()).reportError(any(Throwable.class));
  }

  protected void thenTupleWasFailed()
  {
    verify(outputCollector).fail(tuple);
  }

  protected void thenTupleWasNotFailed()
  {
    verify(outputCollector, never()).fail(tuple);
  }

  protected void verifyEmission(Object... values)
  {
    verify(outputCollector).emit(valuesArgumentCaptor.capture());
    // verify(outputCollector, never()).emit(anyList());
    verify(outputCollector, never()).emit(any(Collection.class), anyList());
    verify(outputCollector, never()).emit(anyString(), anyList());
    verify(outputCollector, never()).emit(any(Tuple.class), anyList());
    verify(outputCollector, never()).emit(anyString(), any(Collection.class), anyList());
    verify(outputCollector, never()).emit(anyString(), any(Tuple.class), anyList());

    verify(outputCollector, never()).emitDirect(any(int.class), anyList());
    verify(outputCollector, never()).emitDirect(any(int.class), any(Collection.class), anyList());
    verify(outputCollector, never()).emitDirect(any(int.class), anyString(), anyList());
    verify(outputCollector, never()).emitDirect(any(int.class), any(Tuple.class), anyList());
    verify(outputCollector, never()).emitDirect(any(int.class), anyString(), any(Collection.class), anyList());
    verify(outputCollector, never()).emitDirect(any(int.class), anyString(), any(Tuple.class), anyList());

    thenTheOutputValuesAre(values);
  }

  protected void verifyEmission(Tuple anchor,
                                Object... values)
  {

    verify(outputCollector, never()).emit(anyList());
    verify(outputCollector, never()).emit(any(Collection.class), anyList());
    verify(outputCollector, never()).emit(anyString(), anyList());

    // verify(outputCollector, never()).emit(any(Tuple.class), anyList());
    verify(outputCollector).emit(eq(anchor), valuesArgumentCaptor.capture());

    verify(outputCollector, never()).emit(anyString(), any(Collection.class), anyList());
    verify(outputCollector, never()).emit(anyString(), any(Tuple.class), anyList());

    verify(outputCollector, never()).emitDirect(any(int.class), anyList());
    verify(outputCollector, never()).emitDirect(any(int.class), any(Collection.class), anyList());
    verify(outputCollector, never()).emitDirect(any(int.class), anyString(), anyList());
    verify(outputCollector, never()).emitDirect(any(int.class), any(Tuple.class), anyList());
    verify(outputCollector, never()).emitDirect(any(int.class), anyString(), any(Collection.class), anyList());
    verify(outputCollector, never()).emitDirect(any(int.class), anyString(), any(Tuple.class), anyList());

    thenTheOutputValuesAre(values);
  }

  protected void verifyEmission(Tuple anchor,
                                String streamId,
                                Object... values)
  {
    verify(outputCollector, never()).emit(anyList());
    verify(outputCollector, never()).emit(any(Collection.class), anyList());
    verify(outputCollector, never()).emit(anyString(), anyList());
    verify(outputCollector, never()).emit(any(Tuple.class), anyList());
    verify(outputCollector, never()).emit(anyString(), any(Collection.class), anyList());
    // verify(outputCollector, never()).emit(anyString(), any(Tuple.class), anyList());
    verify(outputCollector).emit(eq(streamId), eq(anchor), valuesArgumentCaptor.capture());

    verify(outputCollector, never()).emitDirect(any(int.class), anyList());
    verify(outputCollector, never()).emitDirect(any(int.class), any(Collection.class), anyList());
    verify(outputCollector, never()).emitDirect(any(int.class), anyString(), anyList());
    verify(outputCollector, never()).emitDirect(any(int.class), any(Tuple.class), anyList());
    verify(outputCollector, never()).emitDirect(any(int.class), anyString(), any(Collection.class), anyList());
    verify(outputCollector, never()).emitDirect(any(int.class), anyString(), any(Tuple.class), anyList());

    thenTheOutputValuesAre(values);
  }

  protected void verifyNothingWasEmitted()
  {
    verify(outputCollector, never()).emit(anyList());
    verify(outputCollector, never()).emit(any(Collection.class), anyList());
    verify(outputCollector, never()).emit(anyString(), anyList());
    verify(outputCollector, never()).emit(any(Tuple.class), anyList());
    verify(outputCollector, never()).emit(anyString(), any(Collection.class), anyList());
    verify(outputCollector, never()).emit(anyString(), any(Tuple.class), anyList());

    verify(outputCollector, never()).emitDirect(any(int.class), anyList());
    verify(outputCollector, never()).emitDirect(any(int.class), any(Collection.class), anyList());
    verify(outputCollector, never()).emitDirect(any(int.class), anyString(), anyList());
    verify(outputCollector, never()).emitDirect(any(int.class), any(Tuple.class), anyList());
    verify(outputCollector, never()).emitDirect(any(int.class), anyString(), any(Collection.class), anyList());
    verify(outputCollector, never()).emitDirect(any(int.class), anyString(), any(Tuple.class), anyList());
    verify(outputCollector, never()).emitDirect(any(int.class), anyString(), any(Tuple.class), anyList());
  }

  protected void verifyAck()
  {
    verify(outputCollector).ack(tuple);
  }

  protected void verifyNoAck()
  {
    verify(outputCollector, never()).ack(tuple);
  }
}
