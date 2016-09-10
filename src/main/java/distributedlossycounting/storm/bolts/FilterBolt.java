package distributedlossycounting.storm.bolts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import scala.Int;

/**
 * Created by ap0n on 12/3/14.
 */
public class FilterBolt extends BaseRichBolt {

  Logger logger = LoggerFactory.getLogger(FilterBolt.class);
  OutputCollector collector;
  float support;      // support in (0,1)
  float epsilon;      // error in (0,1). error << support
  Map<Integer, Integer> streamLengths;  // Size of the stream (according to setSize)
  long startTimestamp;  // Running time in ms
  int activeCountersCount;

  @Override
  public void prepare(Map config, TopologyContext topologyContext,
                      OutputCollector outputCollector) {
    collector = outputCollector;
    support = Float.parseFloat(config.get("support").toString());
    epsilon = Float.parseFloat(config.get("epsilon").toString());
    streamLengths = new HashMap<Integer, Integer>();
    startTimestamp = 0;
    activeCountersCount = topologyContext.getComponentTasks("counterBolt").size();
  }

  @Override
  public void execute(Tuple tuple) {
    if (tuple.getSourceStreamId().equals("nStream")) {
      int setSize = tuple.getIntegerByField("setSize");
      if (startTimestamp == 0) {
        startTimestamp = new Date().getTime();
      }
      if (!streamLengths.containsKey(setSize)) {
        streamLengths.put(setSize, 1);
      } else {
        streamLengths.put(setSize, streamLengths.get(setSize) + 1);
      }
    } else if (tuple.getSourceStreamId().equals("eofStream")) {
      if (--activeCountersCount > 0) {
        return;
      }
      long runningTime = new Date().getTime() - startTimestamp;

      int totalTuples = 0;
      for (Map.Entry<Integer, Integer> e : streamLengths.entrySet()) {
        totalTuples += e.getValue();
      }

      logger.info("Average speed: "
                  + String.valueOf((double) totalTuples / (double) runningTime * 1000.0)
                  + " tuples/sec");
    } else if (tuple.getSourceStreamId().equals("resultStream")) {
      String itemSet = tuple.getStringByField("itemSet");
      int setSize = itemSet.split(" ").length;
      Integer f = tuple.getIntegerByField("count");

      if (f >= (support - epsilon) * streamLengths.get(setSize)) {
        logger.info(itemSet + ": " + f);
      }
    }
    collector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {}
}
