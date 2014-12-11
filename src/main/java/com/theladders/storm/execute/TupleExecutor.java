package com.theladders.storm.execute;

import java.lang.reflect.Method;
import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

import com.theladders.storm.emit.EmissionStrategy;
import com.theladders.storm.emit.EmissionStrategyFactory;
import com.theladders.storm.execute.exception.TargetBoltExecutionFailed;

public class TupleExecutor
{
  private final TupleValueExtractor tupleValueExtractor;
  private final Executor            executor;
  private final EmissionStrategy    emissionStrategy;
  private final Method              executeMethod;

  private TupleExecutor(Object targetBolt,
                        Method executeMethod)
  {
    this.tupleValueExtractor = TupleValueExtractor.extractorFor(executeMethod);
    this.executor = Executor.with(targetBolt, executeMethod);
    this.emissionStrategy = EmissionStrategyFactory.emissionStrategyFor(executeMethod);
    this.executeMethod = executeMethod;
  }

  public static TupleExecutor executorFor(Object targetBolt,
                                          Method executeMethod)
  {
    return new TupleExecutor(targetBolt, executeMethod);
  }

  public void execute(Tuple tuple,
                      OutputCollector outputCollector)
  {
    Object[] incomingValues = incomingValuesFrom(tuple);
    try
    {
      List<Object> outgoingValues = executeWith(incomingValues);
      if (outgoingValues != null)
      {
        emissionStrategy.emit(tuple, outgoingValues, outputCollector);
      }
      // TODO: only do this if OutputCollector wasn't injected
      outputCollector.ack(tuple);
    }
    catch (TargetBoltExecutionFailed e)
    {
      ExceptionHandler.handle(executeMethod, e.getCause(), outputCollector, tuple);
    }
  }

  private Object[] incomingValuesFrom(Tuple tuple)
  {
    return tupleValueExtractor.valuesFrom(tuple);
  }

  private List<Object> executeWith(Object[] incomingValues)
  {
    return executor.executeWith(incomingValues);
  }
}
