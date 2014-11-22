package com.theladders.storm.emit;

import java.util.List;

import backtype.storm.topology.BasicOutputCollector;

public class StreamEmission implements EmissionStrategy
{

  private final String streamId;

  public StreamEmission(String streamId)
  {
    this.streamId = streamId;
  }

  @Override
  public void emit(List<Object> tuple,
                   BasicOutputCollector outputCollector)
  {
    outputCollector.emit(streamId, tuple);
  }
}
