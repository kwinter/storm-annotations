package com.theladders.storm.execute.field;

import backtype.storm.tuple.Tuple;

public class IndexedFieldExtractor implements FieldExtractor
{
  private final int index;

  public IndexedFieldExtractor(int index)
  {
    this.index = index;
  }

  @Override
  public Object extractFrom(Tuple tuple)
  {
    return tuple.getValue(index);
  };
}
