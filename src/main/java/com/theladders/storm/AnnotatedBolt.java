package com.theladders.storm;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.reflections.Reflections;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.theladders.storm.ack.AckStrategy;
import com.theladders.storm.ack.AckStrategyFactory;
import com.theladders.storm.annotations.Cleanup;
import com.theladders.storm.annotations.Execute;
import com.theladders.storm.annotations.OutputFields;
import com.theladders.storm.emit.EmissionStrategy;
import com.theladders.storm.emit.EmissionStrategyFactory;
import com.theladders.storm.execute.TupleExecutor;
import com.theladders.storm.execute.field.FieldExtractors;
import com.theladders.storm.invoke.CachedMethodInvoker;
import com.theladders.storm.prepare.Preparer;

public class AnnotatedBolt extends BaseRichBolt
{
  private final Object              targetBolt;
  private final Fields              outputFields;
  private final Preparer            preparer;
  private final CachedMethodInvoker cleanupInvoker;

  private OutputCollector           outputCollector;
  private final TupleExecutor       tupleExecutor;
  private final Method              executeMethod;

  public AnnotatedBolt(Object targetBolt)
  {
    this.targetBolt = targetBolt;
    this.outputFields = outputFieldsFor(targetBolt.getClass());
    this.preparer = Preparer.preparerFor(targetBolt.getClass());
    this.cleanupInvoker = CachedMethodInvoker.using(targetBolt.getClass(), Cleanup.class);
    this.executeMethod = executeMethodFor(targetBolt.getClass());
    tupleExecutor = executorFor(targetBolt, preparer, executeMethod);
  }

  // TODO: move this to an annotated bolt factory?
  private static TupleExecutor executorFor(Object targetBolt,
                                           Preparer preparer,
                                           Method executeMethod)
  {
    FieldExtractors fieldExtractors = FieldExtractors.fieldExtractorsFor(executeMethod);
    EmissionStrategy emissionStrategy = EmissionStrategyFactory.emissionStrategyFor(executeMethod,
                                                                                    fieldExtractors,
                                                                                    preparer);
    AckStrategy ackStrategy = AckStrategyFactory.ackStrategyFor(executeMethod, fieldExtractors, preparer);
    return TupleExecutor.executorFor(targetBolt, executeMethod, fieldExtractors, emissionStrategy, ackStrategy);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
  {
    if (outputFields != null)
    {
      outputFieldsDeclarer.declare(outputFields);
    }
  }

  @Override
  public void prepare(Map configMap,
                      TopologyContext topologyContext,
                      OutputCollector collector)
  {
    this.outputCollector = collector;
    preparer.prepareWith(targetBolt, configMap, topologyContext, collector);
  }

  @Override
  public void execute(Tuple tuple)
  {
    tupleExecutor.execute(tuple, outputCollector);
  }

  @Override
  public void cleanup()
  {
    cleanupInvoker.invokeWith(targetBolt);
  }

  private static Fields outputFieldsFor(Class<?> boltClass)
  {
    // TODO: cache this
    OutputFields outputFields = boltClass.getAnnotation(OutputFields.class);
    if (outputFields != null)
    {
      List<String> fields = Arrays.asList(outputFields.value());
      return new Fields(fields);
    }
    return null;
  }

  private static Method executeMethodFor(Class<?> boltClass)
  {
    Set<Method> executeMethods = Reflections.getAllMethods(boltClass, Reflections.withAnnotation(Execute.class));

    if (executeMethods.isEmpty())
    {
      // TODO: better error handling
      throw new RuntimeException("Found no @Execute methods");
    }
    else if (executeMethods.size() > 1)
    {
      // TODO: better error handling
      throw new RuntimeException("Found more than one @Execute methods");
    }
    else
    {
      return executeMethods.iterator().next();
    }
  }

}
