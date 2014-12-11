package com.theladders.storm.execute;

import static com.theladders.storm.util.ExceptionUtils.exceptionIsAssignableToAny;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.ReportedFailedException;
import backtype.storm.tuple.Tuple;

import com.theladders.storm.annotations.FailTupleOn;
import com.theladders.storm.annotations.ReportFailureOn;

// TODO: handle multiple annotations of either
// TODO: execute reflection once and hold onto the results for the entire run
public class ExceptionHandler
{
  private final Method          executeMethod;
  private final Throwable       escapedException;
  private final OutputCollector outputCollector;
  private final Tuple           tuple;
  private FailTupleOn           failTupleOn;
  private ReportFailureOn       reportFailureOn;

  public ExceptionHandler(Method executeMethod,
                          Throwable escapedException,
                          OutputCollector outputCollector,
                          Tuple tuple)
  {
    this.executeMethod = executeMethod;
    this.escapedException = escapedException;
    this.outputCollector = outputCollector;
    this.tuple = tuple;
  }

  public static void handle(Method executeMethod,
                            Throwable escapedException,
                            OutputCollector outputCollector,
                            Tuple tuple)
  {
    new ExceptionHandler(executeMethod, escapedException, outputCollector, tuple).handle();
  }

  public void handle()
  {
    failTupleOn = executeMethod.getAnnotation(FailTupleOn.class);
    reportFailureOn = executeMethod.getAnnotation(ReportFailureOn.class);
    boolean matchesFailTuple = exceptionMatchesFailTuple();
    boolean matchesReportFailure = exceptionMatchesReportFailure();

    if (escapedException instanceof ReportedFailedException)
    {
      reportFailureAndFailTuple();
    }
    else if (escapedException instanceof FailedException)
    {
      failTuple();
    }
    else if (matchesFailTuple && matchesReportFailure)
    {
      if (failTupleAnnotationIsFirst())
      {
        failTuple();
      }
      else
      {
        reportFailureAndFailTuple();
      }
    }
    else if (matchesFailTuple)
    {
      failTuple();
    }
    else if (matchesReportFailure)
    {
      reportFailureAndFailTuple();
    }
    else if (escapedException instanceof RuntimeException)
    {
      throw (RuntimeException) escapedException;
    }
    else
    // checked exception escaped
    {
      // TODO(kw): figure out the best way to handle this
      throw new RuntimeException(escapedException);
    }
  }

  private boolean failTupleAnnotationIsFirst()
  {
    Annotation[] annotations = executeMethod.getAnnotations();
    List<Annotation> annotationList = Arrays.asList(annotations);
    int failTupleIndex = annotationList.indexOf(failTupleOn);
    int reportFailureIndex = annotationList.indexOf(reportFailureOn);
    return failTupleIndex < reportFailureIndex;
  }

  private boolean exceptionMatchesFailTuple()
  {
    if (failTupleOn != null)
    {
      if (exceptionIsAssignableToAny(escapedException, failTupleOn.value()))
      {
        return true;
      }
    }
    return false;
  }

  private boolean exceptionMatchesReportFailure()
  {
    if (reportFailureOn != null)
    {
      if (exceptionIsAssignableToAny(escapedException, reportFailureOn.value()))
      {
        return true;
      }
    }
    return false;
  }

  private void reportFailureAndFailTuple()
  {
    outputCollector.reportError(escapedException);
    failTuple();
    // throw new ReportedFailedException(escapedException);
  }

  private void failTuple()
  {
    outputCollector.fail(tuple);
    // throw new FailedException(escapedException);
  }
}
