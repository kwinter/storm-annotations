package com.theladders.storm.execute;

import java.lang.reflect.Method;
import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

import com.theladders.storm.emit.EmissionStrategy;
import com.theladders.storm.emit.EmissionStrategyFactory;
import com.theladders.storm.execute.exception.TargetBoltExecutionFailed;
import com.theladders.storm.execute.field.FieldExtractors;
import com.theladders.storm.prepare.Preparer;

public class TupleExecutor
{
  private final FieldExtractors  fieldExtractors;
  private final Executor         executor;
  private final EmissionStrategy emissionStrategy;
  private final Method           executeMethod;

  private TupleExecutor(FieldExtractors fieldExtractors,
                        Executor executor,
                        EmissionStrategy emissionStrategy,
                        Method executeMethod)
  {
    this.fieldExtractors = fieldExtractors;
    this.executor = executor;
    this.emissionStrategy = emissionStrategy;
    this.executeMethod = executeMethod;
  }

  public static TupleExecutor executorFor(Object targetBolt,
                                          Method executeMethod,
                                          OutputCollector outputCollector,
                                          Preparer preparer)
  {
    FieldExtractors fieldExtractors = FieldExtractors.fieldExtractorsFor(executeMethod, outputCollector);
    Executor executor = Executor.with(targetBolt, executeMethod);
    EmissionStrategy emissionStrategy = EmissionStrategyFactory.emissionStrategyFor(executeMethod,
                                                                                    fieldExtractors,
                                                                                    preparer);
    return new TupleExecutor(fieldExtractors, executor, emissionStrategy, executeMethod);
  }

  public void execute(Tuple tuple,
                      OutputCollector outputCollector)
  {
    ExecuteParameters incomingValues = incomingValuesFrom(tuple);
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

  private ExecuteParameters incomingValuesFrom(Tuple tuple)
  {
    return fieldExtractors.valuesFrom(tuple);
  }

  private List<Object> executeWith(ExecuteParameters incomingValues)
  {
    return executor.executeWith(incomingValues);
  }
}
