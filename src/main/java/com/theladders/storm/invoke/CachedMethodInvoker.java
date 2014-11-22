package com.theladders.storm.invoke;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

public class CachedMethodInvoker
{
  private final List<Method>                        methods;
  private final Map<Method, List<ParameterMapping>> methodParameterMappings;

  CachedMethodInvoker(List<Method> methods,
                      Map<Method, List<ParameterMapping>> methodParameterMappings)
  {
    this.methods = methods;
    this.methodParameterMappings = methodParameterMappings;
  }

  public static CachedMethodInvoker using(Class<?> targetInstanceClass,
                                          Class<? extends Annotation> targetAnnotation,
                                          Class<?>... possibleInjectedParameterTypes)
  {
    return CachedMethodInvokerFactory.invokerFor(targetInstanceClass, targetAnnotation, possibleInjectedParameterTypes);
  }

  public void invokeWith(Object targetInstance,
                         Object... possibleInjectedParameters)
  {
    for (Method method : methods)
    {
      List<ParameterMapping> parameterMappings = methodParameterMappings.get(method);
      Object[] parameters = new Object[parameterMappings.size()];
      for (int i = 0; i < parameterMappings.size(); i++)
      {
        ParameterMapping parameterMapping = parameterMappings.get(i);
        Class<?> parameterType = parameterMapping.parameterClass;
        Object parameter = possibleInjectedParameters[parameterMapping.indexInPossibleParameters];
        parameters[i] = parameterType.cast(parameter);
      }
      invoke(targetInstance, method, parameters);
    }
  }

  private void invoke(Object targetInstance,
                      Method method,
                      Object[] parameters)
  {
    boolean isAccessible = method.isAccessible();
    try
    {
      // TODO: do we need to change accessibility, or assume types/methods are public?
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

  static class ParameterMapping
  {
    private final int      indexInPossibleParameters;
    private final Class<?> parameterClass;

    public ParameterMapping(int indexInPossibleParameters,
                            Class<?> parameterClass)
    {
      this.indexInPossibleParameters = indexInPossibleParameters;
      this.parameterClass = parameterClass;
    }

  }
}
