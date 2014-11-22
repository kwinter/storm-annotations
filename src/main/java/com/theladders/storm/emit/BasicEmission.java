package com.theladders.storm.emit;

import java.util.List;

import backtype.storm.topology.BasicOutputCollector;

public class BasicEmission implements EmissionStrategy
{


  @Override
  public void emit(List<Object> tuple,
                   BasicOutputCollector outputCollector)
  {
    outputCollector.emit(tuple);
  }
}
