package com.theladders.storm;

import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

import backtype.storm.topology.FailedException;
import backtype.storm.tuple.Values;

import com.theladders.storm.RecordingTestBolt.TestObjectParameter;
import com.theladders.storm.annotations.Execute;
import com.theladders.storm.annotations.FailTupleOn;
import com.theladders.storm.annotations.Field;
import com.theladders.storm.annotations.OutputFields;

public class FailTupleOnTest extends AbstractAnnotatedBoltTest
{
  private Object                 bolt;

  @Before
  public void prepareValues()
  {
    givenInputFields("inputField1", "inputField2");
    givenInputValues(new TestObjectParameter(7), new TestObjectParameter(9));
  }

  @Test
  public void noExceptionConfigRethrowsException()
  {
    bolt = new ExceptionLeakingBolt();

    try
    {
      executeBolt();
      fail("Should have thrown exception");
    }
    catch (MyException e)
    {

    }

    thenFailureWasNotReported();
    thenTupleWasNotFailed();
  }

  @Test
  public void canFailTupleForUncaughtExceptions()
  {
    bolt = new FailTupleOnExceptionBolt();

    executeBolt();

    thenFailureWasNotReported();
    thenTupleWasFailed();
  }

  @Test
  public void canFailTupleForUncaughtExceptionsBasedOnSuperclass()
  {
    bolt = new FailTupleOnExceptionSuperclassBolt();

    executeBolt();

    thenFailureWasNotReported();
    thenTupleWasFailed();
  }

  @Test
  public void failedExceptionsAreFailed()
  {
    bolt = new BoltThatThrowsFailedException();

    executeBolt();

    thenFailureWasNotReported();
    thenTupleWasFailed();
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
