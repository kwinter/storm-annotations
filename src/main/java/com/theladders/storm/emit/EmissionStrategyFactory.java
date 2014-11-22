package com.theladders.storm.emit;

import java.lang.reflect.Method;

import com.theladders.storm.annotations.Stream;
import com.theladders.storm.annotations.Task;

public class EmissionStrategyFactory
{

  public static EmissionStrategy emissionStrategyFor(Method method)
  {
    Stream stream = method.getAnnotation(Stream.class);
    Task task = method.getAnnotation(Task.class);

    if (stream != null && task != null)
    {
      return new StreamAndTaskEmission(stream.value(), task.value());
    }
    else if (stream != null)
    {
      return new StreamEmission(stream.value());
    }
    else if (task != null)
    {
      return new TaskEmission(task.value());
    }
    else
    {
      return new BasicEmission();
    }
  }

}
