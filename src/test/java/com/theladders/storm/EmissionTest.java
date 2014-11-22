package com.theladders.storm;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
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

import com.theladders.storm.annotations.Execute;
import com.theladders.storm.annotations.Field;
import com.theladders.storm.annotations.OutputFields;
import com.theladders.storm.annotations.Stream;
import com.theladders.storm.annotations.Task;

// TODO: test that output fields aren't required
// TODO: test that cleanup is called even with exceptions
public class EmissionTest
{

  @Mock
  private OutputFieldsDeclarer   outputFieldsDeclarer;

  @Captor
  private ArgumentCaptor<Fields> fieldsArgumentCaptor;

  @Mock
  private BasicOutputCollector   basicOutputCollector;

  @Captor
  private ArgumentCaptor<Values> valuesArgumentCaptor;

  private Object                 bolt;

  @Before
  public void setup()
  {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void basicEmit()
  {
    bolt = new TypicalBolt();
    execute();

    verify(basicOutputCollector).emit(valuesArgumentCaptor.capture());

    withTheCorrectValues();
  }

  @Test
  public void withSpecificStream()
  {
    bolt = new WithStream();
    execute();

    verify(basicOutputCollector).emit(eq("anotherStream"), valuesArgumentCaptor.capture());

    withTheCorrectValues();
  }

  @Test
  public void withSpecificTask()
  {
    bolt = new WithTask();
    execute();

    verify(basicOutputCollector).emitDirect(eq(3), valuesArgumentCaptor.capture());

    withTheCorrectValues();
  }

  @Test
  public void withSpecificStreamAndTask()
  {
    bolt = new WithStreamAndTask();
    execute();

    verify(basicOutputCollector).emitDirect(eq(3), eq("anotherStream"), valuesArgumentCaptor.capture());

    withTheCorrectValues();
  }

  private void execute()
  {
    AnnotatedBolt annotatedBolt = new AnnotatedBolt(bolt);
    annotatedBolt.prepare(null, null);
    annotatedBolt.declareOutputFields(outputFieldsDeclarer);

    List list = Arrays.asList(new TestObjectParameter(7), new TestObjectParameter(9));
    GeneralTopologyContext context = mock(GeneralTopologyContext.class);
    when(context.getComponentOutputFields(anyString(), anyString())).thenReturn(new Fields("inputField1", "inputField2"));
    annotatedBolt.execute(new TupleImpl(context, list, 1, "streamId"), basicOutputCollector);
    annotatedBolt.cleanup();
  }

  private void withTheCorrectValues()
  {
    Values values = valuesArgumentCaptor.getValue();
    assertEquals(7, values.get(0));
    assertEquals(9, values.get(1));
  }

  @OutputFields({ "field1", "field2" })
  public static class TypicalBolt
  {
    @Execute
    public Values execute(@Field("inputField1") TestObjectParameter testObject1,
                          @Field("inputField2") TestObjectParameter testObject2)
    {
      return new Values(testObject1.number, testObject2.number);
    }
  }

  @OutputFields({ "field1", "field2" })
  public static class WithStream
  {
    @Execute
    @Stream("anotherStream")
    public Values execute(@Field("inputField1") TestObjectParameter testObject1,
                          @Field("inputField2") TestObjectParameter testObject2)
    {
      return new Values(testObject1.number, testObject2.number);
    }
  }

  @OutputFields({ "field1", "field2" })
  public static class WithTask
  {
    @Execute
    @Task(3)
    public Values execute(@Field("inputField1") TestObjectParameter testObject1,
                          @Field("inputField2") TestObjectParameter testObject2)
    {
      return new Values(testObject1.number, testObject2.number);
    }
  }

  @OutputFields({ "field1", "field2" })
  public static class WithStreamAndTask
  {
    @Execute
    @Stream("anotherStream")
    @Task(3)
    public Values execute(@Field("inputField1") TestObjectParameter testObject1,
                          @Field("inputField2") TestObjectParameter testObject2)
    {
      return new Values(testObject1.number, testObject2.number);
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
