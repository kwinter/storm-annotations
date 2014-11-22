package com.theladders.storm;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
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
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.ReportedFailedException;
import backtype.storm.tuple.Fields;
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
  private BasicOutputCollector   basicOutputCollector;

  @Captor
  private ArgumentCaptor<Values> valuesArgumentCaptor;

  private AnnotatedBolt          annotatedBolt;

  @Before
  public void setup()
  {
    MockitoAnnotations.initMocks(this);
  }

  @Test(expected = MyException.class)
  public void noExceptionConfigRethrowsException()
  {
    ExceptionLeakingBolt bolt = new ExceptionLeakingBolt();
    annotatedBolt = new AnnotatedBolt(bolt);

    executeBolt();
  }

  @Test(expected = ReportedFailedException.class)
  public void canReportFailureForUncaughtExceptions()
  {
    ReportFailureOnExceptionBolt bolt = new ReportFailureOnExceptionBolt();
    annotatedBolt = new AnnotatedBolt(bolt);

    executeBolt();
  }

  @Test(expected = ReportedFailedException.class)
  public void canReportFailureForUncaughtExceptionsBasedOnSuperclass()
  {
    ReportFailureOnExceptionSuperclassBolt bolt = new ReportFailureOnExceptionSuperclassBolt();
    annotatedBolt = new AnnotatedBolt(bolt);

    executeBolt();
  }

  @Test(expected = FailedException.class)
  public void failedExceptionsStillGoThrough()
  {
    BoltThatThrowsFailedException bolt = new BoltThatThrowsFailedException();
    annotatedBolt = new AnnotatedBolt(bolt);

    executeBolt();
  }

  @Test(expected = ReportedFailedException.class)
  public void reportedFailedExceptionsStillGoThrough()
  {
    BoltThatThrowsReportReportedFailedException bolt = new BoltThatThrowsReportReportedFailedException();
    annotatedBolt = new AnnotatedBolt(bolt);

    executeBolt();
  }

  private void executeBolt()
  {
    List list = Arrays.asList(new TestObjectParameter(7), new TestObjectParameter(9));
    GeneralTopologyContext context = mock(GeneralTopologyContext.class);
    when(context.getComponentOutputFields(anyString(), anyString())).thenReturn(new Fields("inputField1", "inputField2"));
    annotatedBolt.execute(new TupleImpl(context, list, 1, "streamId"), basicOutputCollector);
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
