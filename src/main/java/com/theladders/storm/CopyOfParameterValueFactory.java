package com.theladders.storm;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

import backtype.storm.tuple.Tuple;

public class CopyOfParameterValueFactory
{
  private final Tuple  tuple;
  private final Method executeMethod;

  private CopyOfParameterValueFactory(Tuple tuple,
                                Method executeMethod)
  {
    this.tuple = tuple;
    this.executeMethod = executeMethod;
  }

  public static Object[] parameterValuesFor(Tuple tuple,
                                            Method executeMethod)
  {
    return new CopyOfParameterValueFactory(tuple, executeMethod).parameterValues();
  }

  private Object[] parameterValues()
  {
    Annotation[][] allParameterAnnotations = executeMethod.getParameterAnnotations();
    Class<?>[] parameterTypes = executeMethod.getParameterTypes();
    Object[] parameters = new Object[parameterTypes.length];

    for (int i = 0; i < parameterTypes.length; i++)
    {
      Annotation[] parameterAnnotations = allParameterAnnotations[i];
      Object parameter = parameterValueFor(parameterAnnotations);
      parameters[i] = parameter;
    }
    return parameters;
  }

  private Object parameterValueFor(Annotation[] parameterAnnotations)
  {
    com.theladders.storm.annotations.Field fieldAnnotation = fieldAnnotationIn(parameterAnnotations);

    return tuple.getValueByField(fieldAnnotation.value());
  }

  // TODO(kw): move to a utility or use a library, or both
  private com.theladders.storm.annotations.Field fieldAnnotationIn(Annotation[] parameterAnnotations)
  {
    com.theladders.storm.annotations.Field fieldAnnotation = null;
    for (Annotation annotation : parameterAnnotations)
    {
      // TODO(kw): what if there are multiple @Fields?
      if (annotation instanceof com.theladders.storm.annotations.Field)
      {
        fieldAnnotation = (com.theladders.storm.annotations.Field) annotation;
      }
    }
    if (fieldAnnotation == null)
    {
      throw new RuntimeException("No @Field!");
    }
    return fieldAnnotation;
  }

}
