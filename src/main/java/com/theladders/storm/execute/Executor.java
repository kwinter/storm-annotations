package com.theladders.storm.execute;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import backtype.storm.tuple.Values;

// TODO: this is ugly. clean it up
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
  public List<Object> executeWith(Object[] incomingValues)
  {
    Object result = resultOf(incomingValues);

    // TODO(kw): better handling if the return isn't values
    if (result instanceof List)
    {
      return (List<Object>) result;
    }
    if (result.getClass().isArray())
    {
      return arrayValues(result);
    }
    if (result instanceof Iterable)
    {
      if (result instanceof Collection)
      {
        return new ArrayList<>((Collection) result);
      }
      return listFrom((Iterable) result);
    }
    return new Values(result);
  }

  private Object resultOf(Object[] incomingValues)
  {
    Object result;
    try
    {
      result = executeMethod.invoke(targetBolt, incomingValues);
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
    return result;
  }

  private static List<Object> arrayValues(Object result)
  {
    try
    {
      Object[] array = (Object[]) result;
      return Arrays.asList(array);
    }
    catch (ClassCastException e)
    {
      // TODO: better error handling
      throw new RuntimeException("Cannot cast array to Object[], is it an array of objects?  Primitive arrays are not supported",
                                 e);
    }
  }

  private static List<Object> listFrom(Iterable<Object> result)
  {
    List<Object> values = new ArrayList<>();
    Iterator<Object> iterator = result.iterator();
    while (iterator.hasNext())
    {
      values.add(iterator.next());
    }
    return values;
  }
}
