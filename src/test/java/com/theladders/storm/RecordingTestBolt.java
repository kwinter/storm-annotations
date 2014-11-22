package com.theladders.storm;


public abstract class RecordingTestBolt
{

  public boolean wasPrepared;
  public boolean wasExecuted;
  public boolean wasCleanedUp;

  public static class TestObjectParameter
  {
    public final int number;

    public TestObjectParameter(int number)
    {
      this.number = number;
    }
  }
}
