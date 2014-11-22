package com.theladders.storm;

import org.junit.Test;

import backtype.storm.tuple.Values;

import com.theladders.storm.annotations.Execute;
import com.theladders.storm.annotations.Field;
import com.theladders.storm.annotations.OutputFields;

public class PrimitiveInputTest extends AbstractAnnotatedBoltTest
{
  @Test
  public void basicRun()
  {
    givenInputFields("inputField1", "inputField2", "inputField3", "inputField4");
    givenInputValues(5, "test", true, 2.45f);
    whenRunning(new PrimitiveInputBolt());
    thenTheOutputValuesAre(10, "test executed", true, 2.45f);
  }

  @OutputFields({ "field1", "field2", "field3", "field4" })
  public static class PrimitiveInputBolt
  {

    @Execute
    public Values execute(@Field("inputField1") int intNum,
                          @Field("inputField2") String string,
                          @Field("inputField3") boolean bool,
                          @Field("inputField4") float fl)
    {
      return new Values(intNum * 2, string + " executed", bool, fl);
    }

  }

}
