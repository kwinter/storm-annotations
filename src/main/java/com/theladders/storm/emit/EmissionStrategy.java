package com.theladders.storm.emit;

import java.util.List;

import backtype.storm.topology.BasicOutputCollector;

public interface EmissionStrategy
{
  void emit(List<Object> tuple,
            BasicOutputCollector outputCollector);
}
