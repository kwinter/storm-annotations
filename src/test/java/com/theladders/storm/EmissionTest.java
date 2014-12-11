package com.theladders.storm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
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
import java.util.Iterator;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.BasicBoltExecutor;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.theladders.storm.annotations.Execute;
import com.theladders.storm.annotations.Field;
import com.theladders.storm.annotations.OutputFields;
import com.theladders.storm.annotations.Stream;

// TODO: test that output fields aren't required
// TODO: test that cleanup is called even with exceptions
public class EmissionTest
{

  @Mock
  private OutputFieldsDeclarer   outputFieldsDeclarer;

  @Captor
  private ArgumentCaptor<Fields> fieldsArgumentCaptor;

  @Mock
  private OutputCollector        outputCollector;

  @Captor
  private ArgumentCaptor<Values> valuesArgumentCaptor;

  private Object                 bolt;

  private Tuple                  tuple;

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

    verifyBasicEmission();
    withValues(7, 9);
    verifyAck();
  }

  @Test
  public void withSpecificStream()
  {
    bolt = new WithStream();
    execute();

    verifyEmissionOn("anotherStream");
    withValues(7, 9);
    verifyAck();
  }

  @Test
  public void withSingularReturn()
  {
    bolt = new WithSingularReturn();
    execute();

    verifyBasicEmission();
    withValues(7);
    verifyAck();
  }

  @Test
  public void withArrayReturn()
  {
    bolt = new WithArrayReturn();
    execute();

    verifyBasicEmission();
    withValues(7, 9);
    verifyAck();
  }

  @Test
  public void withPrimitiveArrayReturn()
  {
    try
    {
      bolt = new WithPrimitiveArrayReturn();
      execute();
      fail("Should have thrown an exception");
    }
    catch (RuntimeException e)
    {
      // expected
    }
    verifyNothingWasEmitted();
    verifyNoAck();
  }

  @Test
  public void withIterableReturn()
  {
    bolt = new WithIterableReturn();
    execute();

    verifyBasicEmission();
    withValues(7, 9);
    verifyAck();
  }

  @Test
  public void nullReturnEmitsNothing()
  {
    bolt = new WithNullReturn();
    execute();
    verifyNothingWasEmitted();
    verifyAck();
  }

  @Test
  public void voidReturnEmitsNothing()
  {
    bolt = new WithVoidReturn();
    execute();
    verifyNothingWasEmitted();
    verifyAck();
  }

  private void execute()
  {
    List list = Arrays.asList(new TestObjectParameter(7), new TestObjectParameter(9));
    GeneralTopologyContext context = mock(GeneralTopologyContext.class);
    when(context.getComponentOutputFields(anyString(), anyString())).thenReturn(new Fields("inputField1", "inputField2"));
    tuple = new TupleImpl(context, list, 1, "streamId");

    AnnotatedBolt annotatedBolt = new AnnotatedBolt(bolt);
    BasicBoltExecutor basicBoltExecutor = new BasicBoltExecutor(annotatedBolt);
    basicBoltExecutor.prepare(null, null, outputCollector);
    basicBoltExecutor.declareOutputFields(outputFieldsDeclarer);
    basicBoltExecutor.execute(tuple);
    basicBoltExecutor.cleanup();
  }

  private void withValues(Object... expectedValues)
  {
    List<?> values = valuesArgumentCaptor.getValue();
    assertEquals(expectedValues.length, values.size());
    for (int i = 0; i < expectedValues.length; i++)
    {
      assertEquals(expectedValues[i], values.get(i));
    }
  }

  private void verifyBasicEmission()
  {
    verifyEmissionOn(Utils.DEFAULT_STREAM_ID);
  }

  private void verifyEmissionOn(String streamId)
  {
    verify(outputCollector).emit(eq(streamId), eq(tuple), valuesArgumentCaptor.capture());
  }

  private void verifyNothingWasEmitted()
  {
    verify(outputCollector, never()).emit(anyString(), any(Tuple.class), anyList());
  }

  private void verifyAck()
  {
    verify(outputCollector).ack(tuple);
  }

  private void verifyNoAck()
  {
    verify(outputCollector, never()).ack(tuple);
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

  @OutputFields({ "field1" })
  public static class WithSingularReturn
  {
    @Execute
    public int execute(@Field("inputField1") TestObjectParameter testObject1,
                       @Field("inputField2") TestObjectParameter testObject2)
    {
      return testObject1.number;
    }
  }

  @OutputFields({ "field1" })
  public static class WithArrayReturn
  {
    @Execute
    public Integer[] execute(@Field("inputField1") TestObjectParameter testObject1,
                             @Field("inputField2") TestObjectParameter testObject2)
    {
      return new Integer[] { testObject1.number, testObject2.number };
    }
  }

  @OutputFields({ "field1" })
  public static class WithPrimitiveArrayReturn
  {
    @Execute
    public int[] execute(@Field("inputField1") TestObjectParameter testObject1,
                         @Field("inputField2") TestObjectParameter testObject2)
    {
      return new int[] { testObject1.number, testObject2.number };
    }
  }

  @OutputFields({ "field1" })
  public static class WithIterableReturn
  {
    @Execute
    public Iterable<Integer> execute(@Field("inputField1") TestObjectParameter testObject1,
                                     @Field("inputField2") TestObjectParameter testObject2)
    {
      return new SomeIterable(testObject1.number, testObject2.number);
    }
  }

  @OutputFields({ "field1" })
  public static class WithNullReturn
  {
    @Execute
    public Integer execute(@Field("inputField1") TestObjectParameter testObject1,
                           @Field("inputField2") TestObjectParameter testObject2)
    {
      return null;
    }
  }

  @OutputFields({ "field1" })
  public static class WithVoidReturn
  {
    @Execute
    public void execute(@Field("inputField1") TestObjectParameter testObject1,
                        @Field("inputField2") TestObjectParameter testObject2)
    {
      // do nothing
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

  private static class SomeIterable<T> implements Iterable<T>
  {
    private final Collection<T> elements;

    public SomeIterable(T... elements)
    {
      this.elements = Arrays.asList(elements);
    }

    @Override
    public Iterator<T> iterator()
    {
      return elements.iterator();
    }
  }
}
