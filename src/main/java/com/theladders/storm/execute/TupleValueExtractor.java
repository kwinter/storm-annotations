package com.theladders.storm.execute;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Tuple;

import com.theladders.storm.execute.field.FieldExtractor;
import com.theladders.storm.execute.field.IndexedFieldExtractor;
import com.theladders.storm.execute.field.NamedFieldExtractor;
import com.theladders.storm.execute.field.TupleFieldExtractor;

public class TupleValueExtractor
{
  private final List<FieldExtractor> fieldExtractors;

  private TupleValueExtractor(List<FieldExtractor> fieldExtractors)
  {
    this.fieldExtractors = fieldExtractors;
  }

  public static TupleValueExtractor extractorFor(Method executeMethod)
  {
    return new TupleValueExtractor(fieldExtractorsFor(executeMethod));
  }

  private static List<FieldExtractor> fieldExtractorsFor(Method executeMethod)
  {
    Annotation[][] allParameterAnnotations = executeMethod.getParameterAnnotations();
    Class<?>[] parameterTypes = executeMethod.getParameterTypes();
    List<FieldExtractor> fieldExtractors = new ArrayList<>(parameterTypes.length);

    for (int i = 0; i < parameterTypes.length; i++)
    {
      fieldExtractors.add(fieldExtractorFor(parameterTypes[i], allParameterAnnotations[i]));
    }
    return fieldExtractors;
  }

  private static FieldExtractor fieldExtractorFor(Class<?> parameterType,
                                                  Annotation[] parameterAnnotations)
  {
    if (Tuple.class.isAssignableFrom(parameterType))
    {
      return new TupleFieldExtractor();
    }
    com.theladders.storm.annotations.Field fieldAnnotation = fieldAnnotationIn(parameterAnnotations);

    // TODO: handle if name is blank and index is -1, or if both are set
    String fieldName = fieldAnnotation.value();
    if (!fieldName.isEmpty())
    {
      return new NamedFieldExtractor(fieldName);
    }
    int fieldIndex = fieldAnnotation.index();
    if (fieldIndex >= 0)
    {
      return new IndexedFieldExtractor(fieldIndex);
    }
    // TODO: better error handling
    throw new RuntimeException("No suitable @Field values found on parameter " + parameterType);
  }

  public Object[] valuesFrom(Tuple tuple)
  {
    Object[] parameters = new Object[fieldExtractors.size()];

    for (int i = 0; i < fieldExtractors.size(); i++)
    {
      parameters[i] = fieldExtractors.get(i).extractFrom(tuple);
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
