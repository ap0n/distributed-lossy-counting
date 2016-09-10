package distributedlossycounting.storm.bolts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import distributedlossycounting.storm.commons.CounterValue;

/**
 * Created by ap0n on 11/21/14.
 */
public class CounterBolt extends BaseRichBolt {

  Logger logger = LoggerFactory.getLogger(CounterBolt.class);
  OutputCollector collector;
  Map<String, CounterValue> counters;
  float support;  // support in (0,1)
  float epsilon;
  int bucketWidth;
  Map<Integer, Integer> currentBuckets;  // Current bucket per setSize
  Map<Integer, Integer> streamLengths;  // Map of stream lengths. Holds the N of each set size
  int id;
  int activeSpoutsCount;

  @Override
  public void prepare(Map config, TopologyContext topologyContext,
                      OutputCollector outputCollector) {
    collector = outputCollector;
    counters = new HashMap<String, CounterValue>();
    support = Float.parseFloat(config.get("support").toString());
    epsilon = Float.parseFloat(config.get("epsilon").toString());
    bucketWidth = ((Double) Math.ceil(1 / epsilon)).intValue();
    currentBuckets = new HashMap<Integer, Integer>();
    streamLengths = new HashMap<Integer, Integer>();
    id = topologyContext.getThisTaskId();
    activeSpoutsCount = topologyContext.getComponentTasks("wordsSpout").size();
  }

  @Override
  public void execute(Tuple tuple) {

    if (tuple.getSourceStreamId().equals("itemSetsStream")) {
      // Count the itemSet

      String itemSet = tuple.getStringByField("itemSet");
      int setSize = itemSet.split(" ").length;
      if (!streamLengths.containsKey(setSize)) {
        streamLengths.put(setSize, 1);
      } else {
        streamLengths.put(setSize, streamLengths.get(setSize) + 1);
      }

      currentBuckets.put(setSize,
                         ((Double) Math.ceil(streamLengths.get(setSize) / bucketWidth)).intValue());
      if (!counters.containsKey(itemSet)) {
        counters.put(itemSet, new CounterValue(1, currentBuckets.get(setSize) - 1));
      } else {
        CounterValue currentValue = counters.get(itemSet);
        currentValue.setF(currentValue.getF() + 1);
        counters.put(itemSet, currentValue);
      }

      if (streamLengths.get(setSize) % bucketWidth == 0) {  // Prune counters of this setSize

        Iterator<Map.Entry<String, CounterValue>> it = counters.entrySet().iterator();
        while (it.hasNext()) {
          Map.Entry<String, CounterValue> item = it.next();
          int currentSetsize = item.getKey().split(" ").length;
          if (currentSetsize != setSize) {  // Skip different size counters
            continue;
          }
          CounterValue value = item.getValue();
          if (value.getDelta() + value.getF() <= currentBuckets.get(setSize)) {
            it.remove();
          }
        }
      }
    } else if (tuple.getSourceStreamId().equals("eofStream")) {  // Output
      if (--activeSpoutsCount <= 0) {
        Iterator<Map.Entry<String, CounterValue>> it = counters.entrySet().iterator();
        while (it.hasNext()) {
          Map.Entry<String, CounterValue> entry = it.next();
          CounterValue value = entry.getValue();
          int setSize = entry.getKey().split(" ").length;
          if (value.getF() >= (support - epsilon) * streamLengths.get(setSize)) {
            collector.emit("resultStream", new Values(entry.getKey(), value.getF()));
          }
        }
        collector.emit("eofStream", new Values(""));
      }
    }
    collector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream("resultStream", new Fields("itemSet", "count"));
    outputFieldsDeclarer.declareStream("eofStream", new Fields("eof"));
  }
}
