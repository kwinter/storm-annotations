package com.theladders.storm;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;

public abstract class AbstractAnnotatedBoltTest
{

  @Mock
  private OutputFieldsDeclarer   outputFieldsDeclarer;

  @Captor
  private ArgumentCaptor<Fields> fieldsArgumentCaptor;

  @Mock
  private BasicOutputCollector   basicOutputCollector;

  @Captor
  private ArgumentCaptor<Values> valuesArgumentCaptor;

  protected String[]             inputFields;
  protected Object[]             inputValues;

  protected Fields               declaredFields;
  protected Values               returnedValues;

  @Before
  public void setup()
  {
    MockitoAnnotations.initMocks(this);
  }

  public void whenRunning(Object targetBolt)
  {
    AnnotatedBolt annotatedBolt = new AnnotatedBolt(targetBolt);

    annotatedBolt.prepare(new HashMap(), mock(TopologyContext.class));

    declareOutputFields(annotatedBolt);

    execute(annotatedBolt);

    annotatedBolt.cleanup();
  }

  private void declareOutputFields(AnnotatedBolt annotatedBolt)
  {
    annotatedBolt.declareOutputFields(outputFieldsDeclarer);
    verify(outputFieldsDeclarer).declare(fieldsArgumentCaptor.capture());
    declaredFields = fieldsArgumentCaptor.getValue();
  }

  private void execute(AnnotatedBolt annotatedBolt)
  {
    List list = Arrays.asList(inputValues);
    GeneralTopologyContext context = mock(GeneralTopologyContext.class);
    when(context.getComponentOutputFields(anyString(), anyString())).thenReturn(new Fields(inputFields));

    annotatedBolt.execute(new TupleImpl(context, list, 1, "streamId"), basicOutputCollector);
    verify(basicOutputCollector).emit(valuesArgumentCaptor.capture());
    returnedValues = valuesArgumentCaptor.getValue();
  }

  protected void givenInputFields(String... fields)
  {
    inputFields = fields;
  }

  protected void givenInputValues(Object... values)
  {
    inputValues = values;
  }

  protected void thenTheOutputValuesAre(Object... values)
  {
    assertEquals(values.length, returnedValues.size());
    for (int i = 0; i < values.length; i++)
    {
      Object expectedValue = values[i];
      Object actualValue = returnedValues.get(i);
      assertEquals(expectedValue, actualValue);
    }
  }
}
