package com.theladders.storm.execute.field;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

import com.theladders.storm.execute.ExecuteParameters;

public class FieldExtractors
{
  private final List<FieldExtractor> fieldExtractors;
  private final boolean              haveOutputCollector;

  private FieldExtractors(List<FieldExtractor> fieldExtractors,
                          boolean haveOutputCollector)
  {
    this.fieldExtractors = fieldExtractors;
    this.haveOutputCollector = haveOutputCollector;
  }

  public static FieldExtractors fieldExtractorsFor(Method executeMethod,
                                                   OutputCollector outputCollector)
  {
    Annotation[][] allParameterAnnotations = executeMethod.getParameterAnnotations();
    Class<?>[] parameterTypes = executeMethod.getParameterTypes();
    List<FieldExtractor> fieldExtractors = new ArrayList<>(parameterTypes.length);

    for (int i = 0; i < parameterTypes.length; i++)
    {
      fieldExtractors.add(fieldExtractorFor(parameterTypes[i], allParameterAnnotations[i], outputCollector));
    }
    return FieldExtractors.from(fieldExtractors);
  }

  private static FieldExtractor fieldExtractorFor(Class<?> parameterType,
                                                  Annotation[] parameterAnnotations,
                                                  OutputCollector outputCollector)
  {
    if (Tuple.class.isAssignableFrom(parameterType))
    {
      return new TupleFieldExtractor();
    }

    if (parameterType.equals(OutputCollector.class))
    {
      return new OutputCollectorFieldExtractor(outputCollector);
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

  public static FieldExtractors from(List<FieldExtractor> fieldExtractors)
  {
    boolean haveOutputCollector = false;
    for (FieldExtractor fieldExtractor : fieldExtractors)
    {
      if (fieldExtractor instanceof OutputCollectorFieldExtractor)
      {
        haveOutputCollector = true;
      }
    }
    return new FieldExtractors(fieldExtractors, haveOutputCollector);
  }

  public ExecuteParameters valuesFrom(Tuple tuple)
  {
    Object[] parameters = new Object[fieldExtractors.size()];

    for (int i = 0; i < fieldExtractors.size(); i++)
    {
      parameters[i] = fieldExtractors.get(i).extractFrom(tuple);
    }
    return ExecuteParameters.from(parameters, haveOutputCollector);
  }

  public boolean haveOutputCollector()
  {
    return haveOutputCollector;
  }

}
