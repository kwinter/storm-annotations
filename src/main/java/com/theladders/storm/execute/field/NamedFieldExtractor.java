package com.theladders.storm.execute.field;

import backtype.storm.tuple.Tuple;

public class NamedFieldExtractor implements FieldExtractor
{
  private final String name;

  public NamedFieldExtractor(String name)
  {
    this.name = name;
  }

  @Override
  public Object extractFrom(Tuple tuple)
  {
    return tuple.getValueByField(name);
  };
}
