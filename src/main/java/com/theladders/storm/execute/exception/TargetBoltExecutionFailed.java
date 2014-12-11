package com.theladders.storm.execute.exception;

public class TargetBoltExecutionFailed extends RuntimeException
{
  public TargetBoltExecutionFailed(Throwable t)
  {
    super(t);
  }
}
