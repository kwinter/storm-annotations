package com.theladders.storm.util;

public class ExceptionUtils
{

  public static boolean exceptionIsAssignableToAny(RuntimeException exception,
                                                   Class<? extends Throwable>[] throwableClasses)
  {
    for (Class<? extends Throwable> throwableClass : throwableClasses)
    {
      if (throwableClass.isAssignableFrom(exception.getClass()))
      {
        return true;
      }
    }
    return false;
  }
}
