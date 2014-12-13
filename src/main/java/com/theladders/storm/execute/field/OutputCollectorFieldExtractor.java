package com.theladders.storm.execute.field;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

public class OutputCollectorFieldExtractor implements FieldExtractor
{
  private final OutputCollector outputCollector;

  public OutputCollectorFieldExtractor(OutputCollector outputCollector)
  {
    this.outputCollector = outputCollector;
  }

  @Override
  public Object extractFrom(Tuple tuple)
  {
    return outputCollector;
  };
}
