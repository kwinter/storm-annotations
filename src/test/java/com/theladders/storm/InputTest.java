package com.theladders.storm;

import org.junit.Test;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.theladders.storm.annotations.Execute;
import com.theladders.storm.annotations.Field;
import com.theladders.storm.annotations.OutputFields;

// TODO: test if Tuple is present multiple times
public class InputTest extends AbstractAnnotatedBoltTest
{
  @Test
  public void primitiveInput()
  {
    givenInputFields("inputField1", "inputField2", "inputField3", "inputField4");
    givenInputValues(5, "test", true, 2.45f);
    whenRunning(new PrimitiveInputBolt());
    thenTheOutputValuesAre(10, "test executed", true, 2.45f);
  }

  @Test
  public void tupleInput()
  {
    givenInputFields("inputField1", "inputField2", "inputField3", "inputField4");
    givenInputValues(5, "test", true, 2.45f);
    whenRunning(new TupleInputBolt());
    thenTheOutputValuesAre(10, "test executed", true, 2.45f);
  }

  @Test
  public void indexedInput()
  {
    givenInputFields("inputField1", "inputField2", "inputField3", "inputField4");
    givenInputValues(5, "test", true, 2.45f);
    whenRunning(new IndexedInputBolt());
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

  @OutputFields({ "field1", "field2", "field3", "field4" })
  public static class TupleInputBolt
  {

    @Execute
    public Values execute(Tuple tuple)
    {
      int intNum = tuple.getIntegerByField("inputField1");
      String string = tuple.getStringByField("inputField2");
      boolean bool = tuple.getBooleanByField("inputField3");
      float fl = tuple.getFloatByField("inputField4");
      return new Values(intNum * 2, string + " executed", bool, fl);
    }

  }

  @OutputFields({ "field1", "field2", "field3", "field4" })
  public static class IndexedInputBolt
  {

    @Execute
    public Values execute(@Field(index = 0) int intNum,
                          @Field(index = 1) String string,
                          @Field(index = 2) boolean bool,
                          @Field(index = 3) float fl)
    {
      return new Values(intNum * 2, string + " executed", bool, fl);
    }

  }

}
