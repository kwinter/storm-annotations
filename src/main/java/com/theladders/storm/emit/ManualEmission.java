package com.theladders.storm.emit;

import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

public class ManualEmission implements EmissionStrategy
{
  @Override
  public void emit(Tuple inputTuple,
                   List<Object> outgoingTuple,
                   OutputCollector outputCollector)
  {
    // do nothing, the underlying bolt must do it
  }
}
