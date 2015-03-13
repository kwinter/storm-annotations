package com.theladders.storm;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class AnnotationSupportingTopologyBuilder extends TopologyBuilder
{
  public BoltDeclarer setBolt(String id,
                              Object bolt)
  {
    return setBolt(id, new AnnotatedBolt(bolt));
  }

  public BoltDeclarer setBolt(String id,
                              Object bolt,
                              Number parallelismHint)
  {
    return setBolt(id, new AnnotatedBolt(bolt), parallelismHint);
  }

}
