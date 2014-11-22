package com.theladders.storm.execute;

import static com.theladders.storm.util.ExceptionUtils.exceptionIsAssignableToAny;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import backtype.storm.topology.FailedException;
import backtype.storm.topology.ReportedFailedException;

import com.theladders.storm.annotations.FailTupleOn;
import com.theladders.storm.annotations.ReportFailureOn;

// TODO: handle multiple annotations of either
public class ExceptionHandler
{
  private final Method           executeMethod;
  private final RuntimeException escapedException;
  private FailTupleOn            failTupleOn;
  private ReportFailureOn        reportFailureOn;

  public ExceptionHandler(Method executeMethod,
                          RuntimeException escapedException)
  {
    this.executeMethod = executeMethod;
    this.escapedException = escapedException;
  }

  public static void handle(Method executeMethod,
                            RuntimeException escapedException)
  {
    new ExceptionHandler(executeMethod, escapedException).handle();
  }

  public void handle()
  {
    failTupleOn = executeMethod.getAnnotation(FailTupleOn.class);
    reportFailureOn = executeMethod.getAnnotation(ReportFailureOn.class);
    boolean matchesFailTuple = exceptionMatchesFailTuple();
    boolean matchesReportFailure = exceptionMatchesReportFailure();

    if (matchesFailTuple && matchesReportFailure)
    {
      if (failTupleAnnotationIsFirst())
      {
        failTuple();
      }
      else
      {
        reportFailure();
      }
    }
    if (matchesFailTuple)
    {
      failTuple();
    }
    if (matchesReportFailure)
    {
      reportFailure();
    }
    throw escapedException;
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


  private void reportFailure()
  {
    throw new ReportedFailedException(escapedException);
  }

  private void failTuple()
  {
    throw new FailedException(escapedException);
  }
}
