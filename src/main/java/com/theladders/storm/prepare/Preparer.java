package com.theladders.storm.prepare;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;

import com.theladders.storm.annotations.Prepare;
import com.theladders.storm.invoke.CachedMethodInvoker;

public class Preparer
{
  private final CachedMethodInvoker prepareInvoker;
  private final boolean             hasOutputCollector;

  public Preparer(CachedMethodInvoker prepareInvoker,
                  boolean hasOutputCollector)
  {
    this.prepareInvoker = prepareInvoker;
    this.hasOutputCollector = hasOutputCollector;
  }

  public static Preparer preparerFor(Class<?> boltClass)
  {
    CachedMethodInvoker prepareInvoker = CachedMethodInvoker.using(boltClass,
                                                                   Prepare.class,
                                                                   Map.class,
                                                                   TopologyContext.class,
                                                                   OutputCollector.class);

    return new Preparer(prepareInvoker, prepareInvoker.hasParameterOfType(OutputCollector.class));
  }

  public void prepareWith(Object targetInstance,
                         Object... possibleInjectedParameters)
  {
    prepareInvoker.invokeWith(targetInstance, possibleInjectedParameters);
  }

  public boolean hasOutputCollector()
  {
    return hasOutputCollector;
  }
}
