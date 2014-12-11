package com.theladders.storm.emit;

import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

public class BasicEmission implements EmissionStrategy
{
  private final boolean shouldAnchor;

  public BasicEmission(boolean shouldAnchor)
  {
    this.shouldAnchor = shouldAnchor;
  }

  @Override
  public void emit(Tuple inputTuple,
                   List<Object> outgoingTuple,
                   OutputCollector outputCollector)
  {
    outputCollector.emit(outgoingTuple);

    if (this.shouldAnchor)
    {
      outputCollector.emit(inputTuple, outgoingTuple);
    }
  }
}
