package com.theladders.storm;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.ReportedFailedException;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;

import com.theladders.storm.RecordingTestBolt.TestObjectParameter;
import com.theladders.storm.annotations.Execute;
import com.theladders.storm.annotations.Field;
import com.theladders.storm.annotations.OutputFields;
import com.theladders.storm.annotations.ReportFailureOn;

public class ReportErrorOnTest
{

  @Mock
  private OutputFieldsDeclarer   outputFieldsDeclarer;

  @Captor
  private ArgumentCaptor<Fields> fieldsArgumentCaptor;

  @Mock
  private OutputCollector        outputCollector;

  @Captor
  private ArgumentCaptor<Values> valuesArgumentCaptor;

  private AnnotatedBolt          annotatedBolt;
  private Tuple                  tuple;

  @Before
  public void setup()
  {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void noExceptionConfigRethrowsException()
  {
    ExceptionLeakingBolt bolt = new ExceptionLeakingBolt();
    annotatedBolt = new AnnotatedBolt(bolt);

    try
    {
      executeBolt();
      fail("Should have thrown an exception");
    }
    catch (MyException e)
    {

    }
    thenFailureWasNotReported();
    thenTupleWasNotFailed();
  }

  @Test
  public void canReportFailureForUncaughtExceptions()
  {
    ReportFailureOnExceptionBolt bolt = new ReportFailureOnExceptionBolt();
    annotatedBolt = new AnnotatedBolt(bolt);

    executeBolt();

    thenFailureWasReportedFor(MyException.class);
    thenTupleWasFailed();
  }

  @Test
  public void canReportFailureForUncaughtExceptionsBasedOnSuperclass()
  {
    ReportFailureOnExceptionSuperclassBolt bolt = new ReportFailureOnExceptionSuperclassBolt();
    annotatedBolt = new AnnotatedBolt(bolt);

    executeBolt();

    thenFailureWasReportedFor(MyException.class);
    thenTupleWasFailed();
  }

  @Test
  public void failedExceptionsAreFailed()
  {
    BoltThatThrowsFailedException bolt = new BoltThatThrowsFailedException();
    annotatedBolt = new AnnotatedBolt(bolt);

    executeBolt();

    thenFailureWasNotReported();
    thenTupleWasFailed();
  }

  @Test
  public void reportedFailedExceptionsAreReportedAndFailed()
  {
    BoltThatThrowsReportReportedFailedException bolt = new BoltThatThrowsReportReportedFailedException();
    annotatedBolt = new AnnotatedBolt(bolt);

    executeBolt();

    thenFailureWasReportedFor(MyException.class);
    thenTupleWasFailed();
  }

  private void executeBolt()
  {
    List list = Arrays.asList(new TestObjectParameter(7), new TestObjectParameter(9));
    GeneralTopologyContext context = mock(GeneralTopologyContext.class);
    when(context.getComponentOutputFields(anyString(), anyString())).thenReturn(new Fields("inputField1", "inputField2"));
    tuple = new TupleImpl(context, list, 1, "streamId");

    annotatedBolt.prepare(null, null, outputCollector);
    annotatedBolt.declareOutputFields(outputFieldsDeclarer);
    annotatedBolt.execute(tuple);
    annotatedBolt.cleanup();
  }

  private void thenFailureWasReportedFor(Class<? extends Throwable> expectedErrorClass)
  {
    verify(outputCollector).reportError(any(expectedErrorClass));
  }

  private void thenFailureWasNotReported()
  {
    verify(outputCollector, never()).reportError(any(Throwable.class));
  }

  private void thenTupleWasFailed()
  {
    verify(outputCollector).fail(tuple);
  }

  private void thenTupleWasNotFailed()
  {
    verify(outputCollector, never()).fail(tuple);
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
  public static class ReportFailureOnExceptionBolt
  {

    @Execute
    @ReportFailureOn(MyException.class)
    public Values execute(@Field("inputField1") TestObjectParameter testObject1,
                          @Field("inputField2") TestObjectParameter testObject2)
    {
      throw new MyException();
    }
  }

  @OutputFields({ "field1", "field2" })
  public static class ReportFailureOnExceptionSuperclassBolt
  {

    @Execute
    @ReportFailureOn(Exception.class)
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
    @ReportFailureOn(NoSuchMethodError.class)
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
    @ReportFailureOn(NoSuchMethodError.class)
    public Values execute(@Field("inputField1") TestObjectParameter testObject1,
                          @Field("inputField2") TestObjectParameter testObject2)
    {
      throw new FailedException();
    }
  }

  @OutputFields({ "field1", "field2" })
  public static class BoltThatThrowsReportReportedFailedException
  {

    @Execute
    @ReportFailureOn(NoSuchMethodError.class)
    public Values execute(@Field("inputField1") TestObjectParameter testObject1,
                          @Field("inputField2") TestObjectParameter testObject2)
    {
      throw new ReportedFailedException();
    }

  }

  private static class MyException extends RuntimeException
  {

  }
}
