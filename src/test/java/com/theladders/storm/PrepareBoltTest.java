package com.theladders.storm;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.theladders.storm.annotations.Cleanup;
import com.theladders.storm.annotations.Execute;
import com.theladders.storm.annotations.Field;
import com.theladders.storm.annotations.OutputFields;
import com.theladders.storm.annotations.Prepare;

public class PrepareBoltTest
{

  @Mock
  private OutputFieldsDeclarer   outputFieldsDeclarer;

  @Captor
  private ArgumentCaptor<Fields> fieldsArgumentCaptor;

  @Mock
  private BasicOutputCollector   basicOutputCollector;

  @Captor
  private ArgumentCaptor<Values> valuesArgumentCaptor;

  @Before
  public void setup()
  {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void canPrepareWithNoArguments()
  {
    TestBolt bolt = new TestBolt();
    AnnotatedBolt annotatedBolt = new AnnotatedBolt(bolt);

    annotatedBolt.prepare(null, null, mock(OutputCollector.class));

    assertTrue("Should have been prepared", bolt.wasPrepared);

  }

  @Test
  public void canPrepareWithAllArguments()
  {
    TestBoltWithAllPrepareArguments bolt = new TestBoltWithAllPrepareArguments();
    AnnotatedBolt annotatedBolt = new AnnotatedBolt(bolt);

    annotatedBolt.prepare(new HashMap(), mock(TopologyContext.class), mock(OutputCollector.class));

    assertTrue("Should have been prepared", bolt.wasPrepared);

  }

  @Test
  public void canPrepareWithArgumentsInAnyOrder()
  {
    TestBoltWithPrepareArgumentsInReverseOrder bolt = new TestBoltWithPrepareArgumentsInReverseOrder();
    AnnotatedBolt annotatedBolt = new AnnotatedBolt(bolt);

    annotatedBolt.prepare(new HashMap(), mock(TopologyContext.class), mock(OutputCollector.class));

    assertTrue("Should have been prepared", bolt.wasPrepared);

  }

  @OutputFields({ "field1", "field2" })
  public static class TestBolt extends RecordingTestBolt
  {

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

  public static class TestBoltWithAllPrepareArguments extends RecordingTestBolt
  {

    @Prepare
    public void prepare(Map configMap,
                        TopologyContext topologyContext)
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

  public static class TestBoltWithPrepareArgumentsInReverseOrder extends RecordingTestBolt
  {

    @Prepare
    public void prepare(TopologyContext topologyContext,
                        Map configMap)
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

}
