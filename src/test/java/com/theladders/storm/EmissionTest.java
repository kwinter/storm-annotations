package com.theladders.storm;

import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;

import com.theladders.storm.annotations.Execute;
import com.theladders.storm.annotations.Field;
import com.theladders.storm.annotations.ManualAck;
import com.theladders.storm.annotations.OutputFields;
import com.theladders.storm.annotations.Prepare;
import com.theladders.storm.annotations.Stream;
import com.theladders.storm.annotations.Unanchored;

// TODO: test that output fields aren't required
// TODO: test that cleanup is called even with exceptions
public class EmissionTest extends AbstractAnnotatedBoltTest
{
  private Object                 bolt;

  @Before
  public void prepareValues()
  {
    givenInputFields("inputField1", "inputField2");
    givenInputValues(new TestObjectParameter(7), new TestObjectParameter(9));
  }

  @Test
  public void basicEmit()
  {
    bolt = new TypicalBolt();
    execute();

    verifyEmission(tuple, 7, 9);
    verifyAck();
  }

  @Test
  public void withSpecificStream()
  {
    bolt = new WithStream();
    execute();

    verifyEmission(tuple, "anotherStream", 7, 9);
    verifyAck();
  }

  @Test
  public void withSingularReturn()
  {
    bolt = new WithSingularReturn();
    execute();

    verifyEmission(tuple, 7);
    verifyAck();
  }

  @Test
  public void withArrayReturn()
  {
    bolt = new WithArrayReturn();
    execute();

    verifyEmission(tuple, 7, 9);
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

    verifyEmission(tuple, 7, 9);
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

  @Test
  public void prepareOutputCollectorWithReturnEmitsNothing()
  {
    bolt = new BoltWithPrepareOutputCollectorThatReturnsSomething();
    execute();
    verifyNothingWasEmitted();
    verifyAck();
  }

  @Test
  public void executeOutputCollectorWithReturnEmitsNothing()
  {
    bolt = new BoltWithExecuteOutputCollectorThatReturnsSomething();
    execute();
    verifyNothingWasEmitted();
    verifyAck();
  }

  @Test
  public void outputCollectorWithReturnAndHasEmissionDoesEmit()
  {
    bolt = new BoltWithOutputCollectorThatReturnsSomethingAndEmits();
    execute();
    verifyEmission(7, 9);
    verifyAck();
  }

  @Test
  public void outputCollectorWithoutReturnAndHasEmissionDoesEmit()
  {
    bolt = new BoltWithOutputCollectorThatReturnsNothingAndEmits();
    execute();
    verifyEmission(7, 9);
    verifyAck();
  }

  @Test
  public void manualAckWithOutputCollectorDoesntAck()
  {
    bolt = new BoltWithManualAckAndOutputCollector();
    execute();
    verifyEmission(7, 9);
    verifyNoAck();
  }

  @Test(expected = RuntimeException.class)
  public void manualAckWithoutOutputCollectorThrowsException()
  {
    bolt = new BoltWithManualAckAndNoOutputCollector();
    execute();
  }

  @Test
  public void boltWithUnanchoredAnnotationShouldNotAnchor()
  {
    bolt = new WithUnanchoredAnnotation();
    execute();

    verifyEmission(7, 9);
    verifyAck();
  }


  @Test
  public void boltWithSpecificStreamAndUnanchoredAnnotationShouldNotAnchor()
  {
    bolt = new WithStreamAndUnanchored();
    execute();

    verifyEmission("anotherStream", 7, 9);
    verifyAck();
  }

  private void execute()
  {
    whenRunning(bolt);
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

  @OutputFields({ "field1", "field2" })
  public static class BoltWithPrepareOutputCollectorThatReturnsSomething
  {

    @Prepare
    public void prepare(OutputCollector outputCollector)
    {
      // do nothing
    }

    @Execute
    public Values execute(@Field("inputField1") TestObjectParameter testObject1,
                          @Field("inputField2") TestObjectParameter testObject2)
    {
      return new Values(testObject1.number, testObject2.number);
    }
  }

  @OutputFields({ "field1", "field2" })
  public static class BoltWithExecuteOutputCollectorThatReturnsSomething
  {
    @Execute
    public Values execute(@Field("inputField1") TestObjectParameter testObject1,
                          @Field("inputField2") TestObjectParameter testObject2,
                          OutputCollector outputCollector)
    {
      return new Values(testObject1.number, testObject2.number);
    }
  }

  @OutputFields({ "field1", "field2" })
  public static class BoltWithOutputCollectorThatReturnsSomethingAndEmits
  {

    @Execute
    public Values execute(@Field("inputField1") TestObjectParameter testObject1,
                          @Field("inputField2") TestObjectParameter testObject2,
                          OutputCollector outputCollector)
    {
      Values values = new Values(testObject1.number, testObject2.number);
      outputCollector.emit(values);
      return values;
    }
  }

  @OutputFields({ "field1", "field2" })
  public static class BoltWithOutputCollectorThatReturnsNothingAndEmits
  {
    @Execute
    public void execute(@Field("inputField1") TestObjectParameter testObject1,
                        @Field("inputField2") TestObjectParameter testObject2,
                        OutputCollector outputCollector)
    {
      outputCollector.emit(new Values(testObject1.number, testObject2.number));
    }
  }

  @OutputFields({ "field1", "field2" })
  public static class BoltWithManualAckAndOutputCollector
  {
    @Execute
    @ManualAck
    public void execute(@Field("inputField1") TestObjectParameter testObject1,
                        @Field("inputField2") TestObjectParameter testObject2,
                        OutputCollector outputCollector)
    {
      outputCollector.emit(new Values(testObject1.number, testObject2.number));
    }
  }

  @OutputFields({ "field1", "field2" })
  public static class BoltWithManualAckAndNoOutputCollector
  {
    @Execute
    @ManualAck
    public void execute(@Field("inputField1") TestObjectParameter testObject1,
                        @Field("inputField2") TestObjectParameter testObject2)
    {}
  }

  @OutputFields({ "field1" })
  public static class WithUnanchoredAnnotation
  {
    @Execute
    @Unanchored
    public Values execute(@Field("inputField1") TestObjectParameter testObject1,
                          @Field("inputField2") TestObjectParameter testObject2)
    {
      return new Values(testObject1.number, testObject2.number);
    }
  }

  @OutputFields({ "field1", "field2" })
  public static class WithStreamAndUnanchored
  {
    @Execute
    @Stream("anotherStream")
    @Unanchored
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
