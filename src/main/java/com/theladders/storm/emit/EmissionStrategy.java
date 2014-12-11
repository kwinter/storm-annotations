package com.theladders.storm.emit;

import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

public interface EmissionStrategy
{
  void emit(Tuple inputTuple,
            List<Object> outgoingTuple,
            OutputCollector outputCollector);
}
