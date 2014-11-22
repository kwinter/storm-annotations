package com.theladders.storm.execute;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import backtype.storm.tuple.Values;

public class Executor
{
  private final Object targetBolt;
  private final Method executeMethod;

  private Executor(Object targetBolt,
                   Method executeMethod)
  {
    this.targetBolt = targetBolt;
    this.executeMethod = executeMethod;
  }

  public static Executor with(Object targetBolt,
                              Method executeMethod)
  {
    return new Executor(targetBolt, executeMethod);
  }

  // TODO: better error handling, tests
  public Values executeWith(Object[] parameterValues)
  {
    Object result;
    try
    {
      result = executeMethod.invoke(targetBolt, parameterValues);
    }
    catch (IllegalAccessException e)
    {
      throw new RuntimeException("Could not execute " + executeMethod + ", is it public?", e);
    }
    catch (IllegalArgumentException e)
    {
      throw new RuntimeException(e);
    }
    catch (InvocationTargetException e)
    {
      Throwable targetException = e.getTargetException();
      if (targetException instanceof RuntimeException)
      {
        RuntimeException escapedException = (RuntimeException) targetException;
        ExceptionHandler.handle(executeMethod, escapedException);
      }
      // TODO(kw): figure out the best way to handle this
      throw new RuntimeException(e);
    }

    // TODO(kw): better handling if the return isn't values
    Values values = (Values) result;
    return values;
  }

}
