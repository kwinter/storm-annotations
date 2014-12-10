package com.theladders.storm.emit;

import java.lang.reflect.Method;

import com.theladders.storm.annotations.Stream;

public class EmissionStrategyFactory
{

  public static EmissionStrategy emissionStrategyFor(Method method)
  {
    Stream stream = method.getAnnotation(Stream.class);

    if (stream != null)
    {
      return new StreamEmission(stream.value());
    }
    return new BasicEmission();
  }

}
