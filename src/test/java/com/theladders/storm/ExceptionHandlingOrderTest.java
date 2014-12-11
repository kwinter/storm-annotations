package com.theladders.storm;

import org.junit.Test;

import backtype.storm.topology.ReportedFailedException;
import backtype.storm.tuple.Values;

import com.theladders.storm.annotations.Execute;
import com.theladders.storm.annotations.FailTupleOn;
import com.theladders.storm.annotations.ReportFailureOn;

public class ExceptionHandlingOrderTest extends AbstractAnnotatedBoltTest
{
  private Object                 targetBolt;

  @Test
  public void failTupleSubclassBeforeReportFailureSuperclass_failTuple()
  {
    targetBolt = new FailTupleSubclassFirstBolt();
    executeBolt();

    thenFailureWasNotReported();
    thenTupleWasFailed();
  }

  @Test
  public void failTupleSubclassAfterReportFailureSuperclass_reportFailure()
  {
    targetBolt = new FailTupleSubclassSecondBolt();
    executeBolt();

    thenFailureWasReportedFor(ReportedFailedException.class);
    thenTupleWasFailed();
  }

  @Test
  public void reportFailureSubclassBeforeReportFailureSuperclass_reportFailure()
  {
    targetBolt = new ReportFailureSubclassFirstBolt();
    executeBolt();

    thenFailureWasReportedFor(ReportedFailedException.class);
    thenTupleWasFailed();
  }

  @Test
  public void reportFailureSubclassAfterReportFailureSuperclass_failTuple()
  {
    targetBolt = new ReportFailureSubclassSecondBolt();
    executeBolt();

    thenFailureWasNotReported();
    thenTupleWasFailed();
  }

  @Test
  public void failTupleSameclass_before_reportFailure_failTuple()
  {
    targetBolt = new FailTupleSameClassFirstBolt();
    executeBolt();

    thenFailureWasNotReported();
    thenTupleWasFailed();
  }

  @Test
  public void reportFailureSameclass_before_failTuple_reportFailure()
  {
    targetBolt = new ReportFailureSameClassFirstBolt();
    executeBolt();

    thenFailureWasReportedFor(ReportedFailedException.class);
    thenTupleWasFailed();
  }

  private void executeBolt()
  {
    givenInputFields("");
    givenInputValues("");
    whenRunning(targetBolt);
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
