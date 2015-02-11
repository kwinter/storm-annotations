package com.theladders.storm.ack;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

public interface AckStrategy
{

  void ackWith(OutputCollector outputCollector,
               Tuple tuple);

}
