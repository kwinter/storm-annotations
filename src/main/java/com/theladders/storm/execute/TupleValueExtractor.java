package com.theladders.storm.execute;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Tuple;

public class TupleValueExtractor
{
  private final List<String> fieldNames;

  private TupleValueExtractor(List<String> fieldNames)
  {
    this.fieldNames = fieldNames;
  }

  public static TupleValueExtractor extractorFor(Method executeMethod)
  {
    return new TupleValueExtractor(fieldNames(executeMethod));
  }

  private static List<String> fieldNames(Method executeMethod)
  {
    Annotation[][] allParameterAnnotations = executeMethod.getParameterAnnotations();
    Class<?>[] parameterTypes = executeMethod.getParameterTypes();
    List<String> fieldNames = new ArrayList<>(parameterTypes.length);

    for (int i = 0; i < parameterTypes.length; i++)
    {
      Annotation[] parameterAnnotations = allParameterAnnotations[i];
      com.theladders.storm.annotations.Field fieldAnnotation = fieldAnnotationIn(parameterAnnotations);
      fieldNames.add(fieldAnnotation.value());
    }
    return fieldNames;
  }

  public Object[] valuesFrom(Tuple tuple)
  {
    Object[] parameters = new Object[fieldNames.size()];

    for (int i = 0; i < fieldNames.size(); i++)
    {
      parameters[i] = tuple.getValueByField(fieldNames.get(i));
    }
    return parameters;
  }

  // TODO(kw): move to a utility or use a library, or both
  private static com.theladders.storm.annotations.Field fieldAnnotationIn(Annotation[] parameterAnnotations)
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
