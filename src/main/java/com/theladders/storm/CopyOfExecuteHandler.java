package com.theladders.storm;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import com.theladders.storm.execute.ExceptionHandler;

import backtype.storm.tuple.Values;

public class CopyOfExecuteHandler
{
  private final Object   targetBolt;
  private final Method   executeMethod;
  private final Object[] parameterValues;

  private CopyOfExecuteHandler(Object targetBolt,
                         Method executeMethod,
                         Object[] parameterValues)
  {
    this.targetBolt = targetBolt;
    this.executeMethod = executeMethod;
    this.parameterValues = parameterValues;
  }

  public static Values execute(Object targetBolt,
                               Method executeMethod,
                               Object[] parameterValues)
  {
    return new CopyOfExecuteHandler(targetBolt, executeMethod, parameterValues).execute();
  }

  private Values execute()
  {
    Object result;
    try
    {
      result = executeMethod.invoke(targetBolt, parameterValues);
    }
    catch (IllegalAccessException e)
    {
      throw new RuntimeException(e);
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
