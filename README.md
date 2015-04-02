storm-annotations
=================
[![Build Status: master](https://travis-ci.org/kwinter/storm-annotations.svg?branch=master)](https://travis-ci.org/kwinter/storm-annotations)
[![Coverage Status](https://coveralls.io/repos/kwinter/storm-annotations/badge.svg)](https://coveralls.io/r/kwinter/storm-annotations)

Just a little side project to mess with for implementing bolts using annotations

# Usage
By default, it acts very similar to BaseBasicBolt.  Tuples are automatically acked on completion if there are no errors, and emissions are anchored to the incoming tuple.  This behavior can be changed using the @ManualAck and @Unanchored annotations mentioned below.  If OutputCollector is present in either the prepare or execute method(s), you assume control of emission.  Parameters and methods are optional, and you only need to declare what you will actually use.

## Emitting values
In order to emit values, you only need to return:
* a singular primitive or object
* an iterable of objects (this includes Storm's Values class)
* an array of objects (primitive arrays are not supported)
* null, to choose not to emit
Void methods will also not emit anything

By default, all values will be anchored.  To emit unanchored values, see the @Unanchored annotation below.

## Annotations

### @Execute
A method level annotation that identifies the bolt's execute method.  This is the only required annotation, and may only be present on one method.  The method must be public, and may accept any of the following: Tuple, OuputCollector, values annotated with @Field that are extractable via Tuple.getValue or Tuple.getValueByName.

Reminder: if OutputCollector is present as a parameter, you assume control of emissions.

```
public class Bolt {
  @Execute
  public void execute() {}
}
```
is the equivalent of
```
public class Bolt extends BaseBasicBolt {
  public void execute(Tuple tuple, BasicOutputCollector collector) {}
  public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}
```
### @OutputFields
A class level annotation that accepts an array of field names.

```
@OutputFields({ "field1", "field2" })
public static class Bolt
```
is the equivalent of
```
outputFieldsDeclarer.declare(new Fields("field1", "field2"));
```

### @Field
A parameter annotation that is used to extract values from the tuple.  It can pull fields by name or by index.  Indexes are 0 based.
```
  @Execute
  public void execute(@Field("field1") String firstString, @Field(index = 1) String secondString) {}
```
is the equivalent of
```
public void execute(Tuple tuple, BasicOutputCollector collector) {
  String firstString = tuple.getStringByField("field1");
  String secondString = tuple.getString(1);
}
```
### @Prepare
A mehod annotation that identifies the method used to prepare the bolt.  The same parameters are available as in IBolt: Map stormConf, TopologyContext context, OutputCollector collector.

Reminder: if OutputCollector is present as a parameter, you assume control of emissions.
```
  @Prepare
  public void prepare() {}
```
is the equivalent of
```
public void prepare(Map configMap,
                    TopologyContext topologyContext) {}
```
also valid:
```
@Prepare
public void prepare(Map configMap) {}
@Prepare
public void prepare(TopologyContext topologyContext) {}
@Prepare
public void prepare(TopologyContext topologyContext,
                    Map configMap) {}
```

### @Cleanup
A method annotation that identifies the cleanup method of the bolt, the same as IBolt.cleanup()

### @Unanchored
An annotation to be placed on the @Execute method to indicate that values should be emitted unanchored
```
@OutputFields("myFieldName")
public static class Bolt
  @Execute
  @Unanchored
  public String execute()
  {
    return "someValue";
  }
}
```
is the equivalent of
```
public static class TypicalBaseBasicBolt extends BaseRichBolt {
  private OutputCollector outputCollector;
    
  @Override
  public void prepare(Map stormConf,
                      TopologyContext context,
                      OutputCollector collector)
  {
    this.outputCollector = collector;
  }

  @Override
  public void execute(Tuple tuple)
  {
    outputCollector.emit(new Values("someValue"));
    outputCollector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    declarer.declare(new Fields("myFieldName"));
  }
}
```

### @ManualAck
An annotation to be placed on the @Execute method to indicate that no automatic acking should occur.  This is mostly geared towards bolts that may not want to ack/fail right away, but instead defer until later (such as batching).  An OutputCollector must be present in either the @Prepare or @Execute methods in conjuction with this annotation, or there would be no way to ack.

Consider: is acking before the value is emitted a good idea?  Doesn't feel like it.  Perhaps @ManualAck doens't make sense at all, and using OutputCollector should assume control of both emission and ack.  These don't feel equivalent in that respect.

TODO: a better example (like batching), as this one isn't very practical
```
@OutputFields("myFieldName")
public static class Bolt
  @Execute
  @ManualAck
  public String execute(Tuple tuple, OutputCollector outputCollector)
  {
    if (.. flip a coin ..) {
      outputCollector.ack(tuple);
      return "someValue";
    } else {
      throw new RuntimeException();
    }
  }
}
```
is the equivalent of
```
public static class TypicalBaseBasicBolt extends BaseRichBolt {
  private OutputCollector outputCollector;
    
  @Override
  public void prepare(Map stormConf,
                      TopologyContext context,
                      OutputCollector collector)
  {
    this.outputCollector = collector;
  }

  @Override
  public void execute(Tuple tuple)
  {
    if (.. flip a coin ..) {
      outputCollector.emit(tuple, new Values("someValue"));
      outputCollector.ack(tuple);
    } else {
      throw new RuntimeException();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    declarer.declare(new Fields("myFieldName"));
  }
}
```
