package com.theladders.storm.execute;

import java.lang.reflect.Method;
import java.util.List;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Tuple;

import com.theladders.storm.emit.EmissionStrategy;
import com.theladders.storm.emit.EmissionStrategyFactory;

public class TupleExecutor
{
  private final TupleValueExtractor tupleValueExtractor;
  private final Executor            executor;
  private final EmissionStrategy    emissionStrategy;

  private TupleExecutor(Object targetBolt,
                        Method executeMethod)
  {
    this.tupleValueExtractor = TupleValueExtractor.extractorFor(executeMethod);
    this.executor = Executor.with(targetBolt, executeMethod);
    this.emissionStrategy = EmissionStrategyFactory.emissionStrategyFor(executeMethod);
  }

  public static TupleExecutor executorFor(Object targetBolt,
                                          Method executeMethod)
  {
    return new TupleExecutor(targetBolt, executeMethod);
  }

  public void execute(Tuple tuple,
                      BasicOutputCollector outputCollector)
  {
    Object[] incomingValues = incomingValuesFrom(tuple);
    List<Object> outgoingValues = executeWith(incomingValues);
    emissionStrategy.emit(outgoingValues, outputCollector);
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
