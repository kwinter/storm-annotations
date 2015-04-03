package com.theladders.storm.exception;

import java.util.Arrays;
import java.util.Collection;

public class NonFatal implements ExceptionClassifier
{
  private static final Collection<Class<? extends Throwable>> FATAL_EXCEPTIONS = Arrays.asList(VirtualMachineError.class,
                                                                                               ThreadDeath.class,
                                                                                               InterruptedException.class,
                                                                                               LinkageError.class);

  @Override
  public boolean isSatisfiedBy(Throwable t)
  {
    for (Class<? extends Throwable> fatal : FATAL_EXCEPTIONS)
    {
      if (fatal.isAssignableFrom(t.getClass()))
      {
        return false;
      }
    }
    return true;
  }
}
