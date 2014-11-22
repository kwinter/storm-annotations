package com.theladders.storm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;

import com.theladders.storm.annotations.Cleanup;
import com.theladders.storm.annotations.Execute;
import com.theladders.storm.annotations.Field;
import com.theladders.storm.annotations.OutputFields;
import com.theladders.storm.annotations.Prepare;

// TODO: test that output fields aren't required
// TODO: test that cleanup is called even with exceptions
public class AnnotatedBoltTest
{

  @Mock
  private OutputFieldsDeclarer   outputFieldsDeclarer;

  @Captor
  private ArgumentCaptor<Fields> fieldsArgumentCaptor;

  @Mock
  private BasicOutputCollector   basicOutputCollector;

  @Captor
  private ArgumentCaptor<Values> valuesArgumentCaptor;

  @Before
  public void setup()
  {
    MockitoAnnotations.initMocks(this);
  }
  @Test
  public void basicRun()
  {
    TestBolt bolt = new TestBolt();
    AnnotatedBolt annotatedBolt = new AnnotatedBolt(bolt);

    annotatedBolt.prepare(null, null);

    assertTrue("Should have been prepared", bolt.wasPrepared);

    annotatedBolt.declareOutputFields(outputFieldsDeclarer);

    verify(outputFieldsDeclarer).declare(fieldsArgumentCaptor.capture());
    Fields fields = fieldsArgumentCaptor.getValue();
    assertEquals(Arrays.asList("field1", "field2"), fields.toList());

    List list = Arrays.asList(new TestObjectParameter(7), new TestObjectParameter(9));
    GeneralTopologyContext context = mock(GeneralTopologyContext.class);
    when(context.getComponentOutputFields(anyString(), anyString())).thenReturn(new Fields("inputField1", "inputField2"));
    annotatedBolt.execute(new TupleImpl(context, list, 1, "streamId"), basicOutputCollector);

    assertTrue("Should have been executed", bolt.wasExecuted);

    verify(basicOutputCollector).emit(valuesArgumentCaptor.capture());

    Values values = valuesArgumentCaptor.getValue();
    assertEquals(7, values.get(0));
    assertEquals(9, values.get(1));

    annotatedBolt.cleanup();

    assertTrue("Should have been cleaned up", bolt.wasCleanedUp);
  }

  @OutputFields({ "field1", "field2" })
  public static class TestBolt
  {

    public boolean wasPrepared;
    public boolean wasExecuted;
    public boolean wasCleanedUp;

    @Prepare
    public void prepare()
    {
      wasPrepared = true;
    }

    @Execute
    public Values execute(@Field("inputField1") TestObjectParameter testObject1,
                          @Field("inputField2") TestObjectParameter testObject2)
    {
      wasExecuted = true;
      return new Values(testObject1.number, testObject2.number);
    }

    @Cleanup
    public void clean()
    {
      wasCleanedUp = true;
    }
  }

  private static class TestObjectParameter
  {
    public final int number;

    public TestObjectParameter(int number)
    {
      this.number = number;
    }
  }
}
