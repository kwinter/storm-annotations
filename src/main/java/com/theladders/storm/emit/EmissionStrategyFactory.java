package com.theladders.storm.emit;

import java.lang.reflect.Method;

import com.theladders.storm.annotations.Stream;
import com.theladders.storm.execute.field.FieldExtractors;
import com.theladders.storm.prepare.Preparer;

public class EmissionStrategyFactory
{

  // TODO: determine whether or not to ack
  public static EmissionStrategy emissionStrategyFor(Method method,
                                                     FieldExtractors fieldExtractors,
                                                     Preparer preparer)
  {
    if (fieldExtractors.haveOutputCollector() || preparer.hasOutputCollector())
    {
      return new ManualEmission();
    }

    Stream stream = method.getAnnotation(Stream.class);

    if (stream != null)
    {
      return new StreamEmission(true, stream.value());
    }
    return new BasicEmission(true);
  }

}
