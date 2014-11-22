package com.theladders.storm.invoke;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.theladders.storm.invoke.CachedMethodInvoker.ParameterMapping;
import com.theladders.storm.util.Reflection;

public class CachedMethodInvokerFactory
{

  public static CachedMethodInvoker invokerFor(Class<?> targetInstanceClass,
                                               Class<? extends Annotation> targetAnnotation,
                                               Class<?>... possibleInjectedParameterTypes)
  {
    Map<Method, List<ParameterMapping>> methodParameterMappings = new HashMap<>();
    List<Method> methods = Reflection.getMethodsAnnotatedWith(targetInstanceClass, targetAnnotation);
    for (Method method : methods)
    {
      Class<?>[] requestedParameterTypes = method.getParameterTypes();
      List<ParameterMapping> parameterMappings = new ArrayList<>(requestedParameterTypes.length);
      for (int i = 0; i < requestedParameterTypes.length; i++)
      {
        Class<?> requestedParameterType = requestedParameterTypes[i];
        ParameterMapping parameterMapping = parameterFor(requestedParameterType, possibleInjectedParameterTypes);
        parameterMappings.add(parameterMapping);
      }
      methodParameterMappings.put(method, parameterMappings);
    }
    return new CachedMethodInvoker(methods, methodParameterMappings);
  }

  private static ParameterMapping parameterFor(Class<?> requestedParameterType,
                                               Class<?>... possibleInjectedParameterTypes)
  {
    for (int j = 0; j < possibleInjectedParameterTypes.length; j++)
    {
      Class<?> possibleInjectedParameterType = possibleInjectedParameterTypes[j];
      // TODO(kw): what if there are multiple matches?
      if (requestedParameterType.isAssignableFrom(possibleInjectedParameterType))
      {
        return new ParameterMapping(j, requestedParameterType);
      }
    }
    throw new RuntimeException("No possible value found for: " + requestedParameterType.getName());
  }

}
