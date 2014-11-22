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
import com.theladders.storm.annotations.FailTupleOn;
import com.theladders.storm.annotations.ReportFailureOn;

public class ExceptionHandlingOrderTest extends AbstractAnnotatedBoltTest
{

  @Mock
  private OutputFieldsDeclarer   outputFieldsDeclarer;

  @Captor
  private ArgumentCaptor<Fields> fieldsArgumentCaptor;

  @Mock
  private BasicOutputCollector   basicOutputCollector;

  @Captor
  private ArgumentCaptor<Values> valuesArgumentCaptor;

  private Object                 targetBolt;

  @Override
  @Before
  public void setup()
  {
    MockitoAnnotations.initMocks(this);
  }

  @Test(expected = FailedException.class)
  public void failTupleSubclassBeforeReportFailureSuperclass_failTuple()
  {
    targetBolt = new FailTupleSubclassFirstBolt();
    executeBolt();
  }

  @Test(expected = ReportedFailedException.class)
  public void failTupleSubclassAfterReportFailureSuperclass_reportFailure()
  {
    targetBolt = new FailTupleSubclassSecondBolt();
    executeBolt();
  }

  @Test(expected = ReportedFailedException.class)
  public void reportFailureSubclassBeforeReportFailureSuperclass_reportFailure()
  {
    targetBolt = new ReportFailureSubclassFirstBolt();
    executeBolt();
  }

  @Test(expected = FailedException.class)
  public void reportFailureSubclassAfterReportFailureSuperclass_failTuple()
  {
    targetBolt = new ReportFailureSubclassSecondBolt();
    executeBolt();
  }

  @Test(expected = FailedException.class)
  public void failTupleSameclass_before_reportFailure_failTuple()
  {
    targetBolt = new FailTupleSameClassFirstBolt();
    executeBolt();
  }

  @Test(expected = ReportedFailedException.class)
  public void reportFailureSameclass_before_failTuple_reportFailure()
  {
    targetBolt = new ReportFailureSameClassFirstBolt();
    executeBolt();
  }

  private void executeBolt()
  {
    List list = Arrays.asList(new TestObjectParameter(7), new TestObjectParameter(9));
    GeneralTopologyContext context = mock(GeneralTopologyContext.class);
    when(context.getComponentOutputFields(anyString(), anyString())).thenReturn(new Fields("inputField1", "inputField2"));
    new AnnotatedBolt(targetBolt).execute(new TupleImpl(context, list, 1, "streamId"), basicOutputCollector);
  }

  public static class FailTupleSubclassFirstBolt
  {

    @Execute
    @FailTupleOn(MyException.class)
    @ReportFailureOn(Exception.class)
    public Values execute()
    {
      throw new MyException();
    }
  }

  public static class FailTupleSubclassSecondBolt
  {

    @Execute
    @ReportFailureOn(Exception.class)
    @FailTupleOn(MyException.class)
    public Values execute()
    {
      throw new MyException();
    }
  }

  public static class ReportFailureSubclassFirstBolt
  {

    @Execute
    @ReportFailureOn(MyException.class)
    @FailTupleOn(Exception.class)
    public Values execute()
    {
      throw new MyException();
    }
  }

  public static class ReportFailureSubclassSecondBolt
  {

    @Execute
    @FailTupleOn(Exception.class)
    @ReportFailureOn(MyException.class)
    public Values execute()
    {
      throw new MyException();
    }
  }

  public static class FailTupleSameClassFirstBolt
  {

    @Execute
    @FailTupleOn(MyException.class)
    @ReportFailureOn(MyException.class)
    public Values execute()
    {
      throw new MyException();
    }
  }

  public static class ReportFailureSameClassFirstBolt
  {

    @Execute
    @ReportFailureOn(MyException.class)
    @FailTupleOn(MyException.class)
    public Values execute()
    {
      throw new MyException();
    }
  }

  private static class MyException extends RuntimeException
  {

  }

}
