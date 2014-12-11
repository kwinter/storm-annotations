package com.theladders.storm.emit;

import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

public class StreamEmission implements EmissionStrategy
{
  private final boolean shouldAnchor;
  private final String  streamId;

  public StreamEmission(boolean shouldAnchor,
                        String streamId)
  {
    this.shouldAnchor = shouldAnchor;
    this.streamId = streamId;
  }

  @Override
  public void emit(Tuple inputTuple,
                   List<Object> outgoingTuple,
                   OutputCollector outputCollector)
  {
    outputCollector.emit(streamId, outgoingTuple);

    if (this.shouldAnchor)
    {
      outputCollector.emit(streamId, inputTuple, outgoingTuple);
    }
  }
}