package com.theladders.storm.emit;

import java.util.List;

import backtype.storm.topology.BasicOutputCollector;

public class StreamAndTaskEmission implements EmissionStrategy
{
  private final String streamId;
  private final int    taskId;

  public StreamAndTaskEmission(String streamId,
                               int taskId)
  {
    this.streamId = streamId;
    this.taskId = taskId;
  }

  @Override
  public void emit(List<Object> tuple,
                   BasicOutputCollector outputCollector)
  {
    outputCollector.emitDirect(taskId, streamId, tuple);
  }
}
