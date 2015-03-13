package com.theladders.storm.execute;

public class ExecuteParameters
{
  private final Object[] parameters;
  // TODO: this isn't used now
  private final boolean  haveOutputCollector;

  private ExecuteParameters(Object[] parameters,
                            boolean haveOutputCollector)
  {
    this.parameters = parameters;
    this.haveOutputCollector = haveOutputCollector;
  }

  public static ExecuteParameters from(Object[] parameters,
                                       boolean haveOutputCollector)
  {
    return new ExecuteParameters(parameters, haveOutputCollector);
  }

  public Object[] actualParameters()
  {
    return parameters;
  }

  public boolean haveOutputCollector()
  {
    return haveOutputCollector;
  }

}
