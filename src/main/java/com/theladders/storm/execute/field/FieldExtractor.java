package com.theladders.storm.execute.field;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

public interface FieldExtractor
{
  Object extractFrom(Tuple tuple,
                     OutputCollector outputCollector);
}
