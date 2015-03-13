package com.theladders.storm.execute;

import java.lang.reflect.Method;
import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

import com.theladders.storm.ack.AckStrategy;
import com.theladders.storm.emit.EmissionStrategy;
import com.theladders.storm.execute.exception.TargetBoltExecutionFailed;
import com.theladders.storm.execute.field.FieldExtractors;

public class TupleExecutor
{
  private final FieldExtractors  fieldExtractors;
  private final Executor         executor;
  private final EmissionStrategy emissionStrategy;
  private final AckStrategy      ackStrategy;
  private final Method           executeMethod;

  private TupleExecutor(FieldExtractors fieldExtractors,
                        Executor executor,
                        EmissionStrategy emissionStrategy,
                        AckStrategy ackStrategy,
                        Method executeMethod)
  {
    this.fieldExtractors = fieldExtractors;
    this.executor = executor;
    this.emissionStrategy = emissionStrategy;
    this.ackStrategy = ackStrategy;
    this.executeMethod = executeMethod;
  }

  public static TupleExecutor executorFor(Object targetBolt,
                                          Method executeMethod,
                                          FieldExtractors fieldExtractors,
                                          EmissionStrategy emissionStrategy,
                                          AckStrategy ackStrategy)
  {
    Executor executor = Executor.with(targetBolt, executeMethod);
    return new TupleExecutor(fieldExtractors, executor, emissionStrategy, ackStrategy, executeMethod);
  }

  public void execute(Tuple tuple,
                      OutputCollector outputCollector)
  {
    ExecuteParameters parameterValues = parameterValuesFrom(tuple, outputCollector);
    try
    {
      List<Object> outgoingValues = executeWith(parameterValues);
      if (outgoingValues != null)
      {
        emissionStrategy.emit(tuple, outgoingValues, outputCollector);
      }
      ackStrategy.ackWith(outputCollector, tuple);
    }
    catch (TargetBoltExecutionFailed e)
    {
      ExceptionHandler.handle(executeMethod, e.getCause(), outputCollector, tuple);
    }
  }

  private ExecuteParameters parameterValuesFrom(Tuple tuple,
                                                OutputCollector outputCollector)
  {
    return fieldExtractors.valuesFrom(tuple, outputCollector);
  }

  private List<Object> executeWith(ExecuteParameters incomingValues)
  {
    return executor.executeWith(incomingValues);
  }
}
