package com.theladders.storm.invoke;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import com.theladders.storm.util.Reflection;

public class MethodInvoker
{

  public static void invokeMethodWith(Object targetInstance,
                                      Class<? extends Annotation> targetAnnotation)
  {
    List<Method> methods = Reflection.getMethodsAnnotatedWith(targetInstance.getClass(), targetAnnotation);
    for (Method method : methods)
    {
      // TODO(kw): pass config map and topology context if they are in the parameter list
      boolean isAccessible = method.isAccessible();
      try
      {
        method.setAccessible(true);
        method.invoke(targetInstance);
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
        throw new RuntimeException(e);
      }
      finally
      {
        method.setAccessible(isAccessible);
      }
    }
  }

  public static void invokeMethodWith(Object targetInstance,
                                Class<? extends Annotation> targetAnnotation,
                                Object... possibleInjectedParameters)
  {
    List<Method> methods = Reflection.getMethodsAnnotatedWith(targetInstance.getClass(), targetAnnotation);
    for (Method method : methods)
    {
      Class<?>[] parameterTypes = method.getParameterTypes();
      Object[] parameters = new Object[parameterTypes.length];
      Object valueToInject = null;
      for (int i = 0; i < parameterTypes.length; i++)
      {
        Class<?> parameterType = parameterTypes[i];
        for (Object possibleInjectedParameter : possibleInjectedParameters)
        {
          // TODO(kw): what if there are multiple matches?
          if (parameterType.isAssignableFrom(possibleInjectedParameter.getClass()))
          {
            valueToInject = possibleInjectedParameter;
          }
        }
        if (valueToInject == null)
        {
          throw new RuntimeException("No possible value found for: " + parameterType.getName());
        }
        parameters[i] = parameterType.cast(valueToInject);
      }
      boolean isAccessible = method.isAccessible();
      try
      {
        method.setAccessible(true);
        method.invoke(targetInstance, parameters);
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
        throw new RuntimeException(e);
      }
      finally
      {
        method.setAccessible(isAccessible);
      }
    }
  }

}
