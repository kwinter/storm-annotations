package com.theladders.storm.ack;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

public class AckOnSuccess implements AckStrategy
{
  @Override
  public void ackWith(OutputCollector outputCollector,
                      Tuple tuple)
  {
    outputCollector.ack(tuple);
  }
}
