package com.theladders.storm.emit;

import java.util.List;

import backtype.storm.topology.BasicOutputCollector;

public class TaskEmission implements EmissionStrategy
{

  private final int taskId;

  public TaskEmission(int taskId)
  {
    this.taskId = taskId;
  }

  @Override
  public void emit(List<Object> tuple,
                   BasicOutputCollector outputCollector)
  {
    outputCollector.emitDirect(taskId, tuple);
  }
}
