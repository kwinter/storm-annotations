package com.theladders.storm.execute.field;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

public class TupleFieldExtractor implements FieldExtractor
{

  @Override
  public Object extractFrom(Tuple tuple,
                            OutputCollector outputCollector)
  {
    return tuple;
  };
}
