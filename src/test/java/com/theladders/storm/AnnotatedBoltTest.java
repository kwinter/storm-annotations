package com.theladders.storm;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import backtype.storm.tuple.Values;

import com.theladders.storm.annotations.Cleanup;
import com.theladders.storm.annotations.Execute;
import com.theladders.storm.annotations.Field;
import com.theladders.storm.annotations.OutputFields;
import com.theladders.storm.annotations.Prepare;

// TODO: test that output fields aren't required
// TODO: test that cleanup is called even with exceptions
public class AnnotatedBoltTest extends AbstractAnnotatedBoltTest
{

  @Test
  public void basicRun()
  {
    TestBolt bolt = new TestBolt();

    givenInputFields("inputField1", "inputField2");
    givenInputValues(new TestObjectParameter(7), new TestObjectParameter(9));
    whenRunning(bolt);

    assertTrue("Should have been prepared", bolt.wasPrepared);
    assertTrue("Should have been executed", bolt.wasExecuted);
    verifyEmission(tuple, 7, 9);
    assertTrue("Should have been cleaned up", bolt.wasCleanedUp);
  }

  @OutputFields({ "field1", "field2" })
  public static class TestBolt
  {

    public boolean wasPrepared;
    public boolean wasExecuted;
    public boolean wasCleanedUp;

    @Prepare
    public void prepare()
    {
      wasPrepared = true;
    }

    @Execute
    public Values execute(@Field("inputField1") TestObjectParameter testObject1,
                          @Field("inputField2") TestObjectParameter testObject2)
    {
      wasExecuted = true;
      return new Values(testObject1.number, testObject2.number);
    }

    @Cleanup
    public void clean()
    {
      wasCleanedUp = true;
    }
  }

  private static class TestObjectParameter
  {
    public final int number;

    public TestObjectParameter(int number)
    {
      this.number = number;
    }
  }
}
