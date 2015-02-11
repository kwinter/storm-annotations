package com.theladders.storm.ack;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

public class ManualAck implements AckStrategy
{
  @Override
  public void ackWith(OutputCollector outputCollector,
                      Tuple tuple)
  {
    // do nothing, must be handled by the client bolt
  }
}
