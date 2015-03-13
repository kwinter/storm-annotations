package com.theladders.storm.execute;

public class ExecuteParameters
{
  private final Object[] parameters;

  private ExecuteParameters(Object[] parameters)
  {
    this.parameters = parameters;
  }

  public static ExecuteParameters from(Object[] parameters)
  {
    return new ExecuteParameters(parameters);
  }

  public Object[] actualParameters()
  {
    return parameters;
  }

}
