package com.theladders.storm;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.theladders.storm.RecordingTestBolt.TestObjectParameter;
import com.theladders.storm.annotations.Execute;
import com.theladders.storm.annotations.FailTupleOn;
import com.theladders.storm.annotations.Field;
import com.theladders.storm.annotations.OutputFields;

public class FailTupleOnTest extends AbstractAnnotatedBoltTest
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

  @Override
  @Before
  public void setup()
  {
    MockitoAnnotations.initMocks(this);
    givenInputFields("inputField1", "inputField2");
    givenInputValues(new TestObjectParameter(7), new TestObjectParameter(9));
  }

  @Test(expected = MyException.class)
  public void noExceptionConfigRethrowsException()
  {
    bolt = new ExceptionLeakingBolt();

    executeBolt();
  }

  @Test(expected = FailedException.class)
  public void canFailTupleForUncaughtExceptions()
  {
    bolt = new FailTupleOnExceptionBolt();

    executeBolt();
  }

  @Test(expected = FailedException.class)
  public void canFailTupleForUncaughtExceptionsBasedOnSuperclass()
  {
    bolt = new FailTupleOnExceptionSuperclassBolt();

    executeBolt();
  }

  @Test(expected = FailedException.class)
  public void failedExceptionsStillGoThrough()
  {
    bolt = new BoltThatThrowsFailedException();

    executeBolt();
  }

  private void executeBolt()
  {
    whenRunning(bolt);
  }

  @OutputFields({ "field1", "field2" })
  public static class ExceptionLeakingBolt
  {

    @Execute
    public Values execute(@Field("inputField1") TestObjectParameter testObject1,
                          @Field("inputField2") TestObjectParameter testObject2)
    {
      throw new MyException();
    }

  }

  @OutputFields({ "field1", "field2" })
  public static class FailTupleOnExceptionBolt
  {

    @Execute
    @FailTupleOn(MyException.class)
    public Values execute(@Field("inputField1") TestObjectParameter testObject1,
                          @Field("inputField2") TestObjectParameter testObject2)
    {
      throw new MyException();
    }

  }

  @OutputFields({ "field1", "field2" })
  public static class FailTupleOnExceptionSuperclassBolt
  {

    @Execute
    @FailTupleOn(Exception.class)
    public Values execute(@Field("inputField1") TestObjectParameter testObject1,
                          @Field("inputField2") TestObjectParameter testObject2)
    {
      throw new MyException();
    }
  }

  @OutputFields({ "field1", "field2" })
  public static class FailTupleWithUnmatchedExceptionBolt
  {

    @Execute
    @FailTupleOn(NoSuchMethodError.class)
    public Values execute(@Field("inputField1") TestObjectParameter testObject1,
                          @Field("inputField2") TestObjectParameter testObject2)
    {
      throw new MyException();
    }
  }

  @OutputFields({ "field1", "field2" })
  public static class BoltThatThrowsFailedException
  {

    @Execute
    @FailTupleOn(NoSuchMethodError.class)
    public Values execute(@Field("inputField1") TestObjectParameter testObject1,
                          @Field("inputField2") TestObjectParameter testObject2)
    {
      throw new FailedException();
    }
  }

  private static class MyException extends RuntimeException
  {

  }
}
