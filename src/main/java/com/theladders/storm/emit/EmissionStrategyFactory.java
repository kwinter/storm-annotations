package com.theladders.storm.emit;

import java.lang.reflect.Method;

import com.theladders.storm.annotations.Stream;

public class EmissionStrategyFactory
{

  // TODO: determine whether or not to ack
  public static EmissionStrategy emissionStrategyFor(Method method)
  {
    Stream stream = method.getAnnotation(Stream.class);

    if (stream != null)
    {
      return new StreamEmission(true, stream.value());
    }
    return new BasicEmission(true);
  }

}
