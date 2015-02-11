package com.theladders.storm.ack;

import java.lang.reflect.Method;

import com.theladders.storm.annotations.ManualAck;
import com.theladders.storm.execute.field.FieldExtractors;
import com.theladders.storm.prepare.Preparer;

public class AckStrategyFactory
{
  public static AckStrategy ackStrategyFor(Method executeMethod,
                                           FieldExtractors fieldExtractors,
                                           Preparer preparer)
  {
    if (executeMethod.isAnnotationPresent(ManualAck.class))
    {
      if (outputCollectorIsntInjected(preparer, fieldExtractors))
      {
        throw new RuntimeException("@ManualAck can't be used without prepare or execute having OutputCollector, there'd be no way to ack!");
      }
      return new com.theladders.storm.ack.ManualAck();
    }
    return new AckOnSuccess();
  }

  private static boolean outputCollectorIsntInjected(Preparer preparer,
                                                     FieldExtractors fieldExtractors)
  {
    return !(fieldExtractors.haveOutputCollector() || preparer.hasOutputCollector());
  }
}
