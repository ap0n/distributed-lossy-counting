package distributedlossycounting.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import distributedlossycounting.storm.bolts.CounterBolt;
import distributedlossycounting.storm.bolts.FilterBolt;
import distributedlossycounting.storm.spouts.ReadFromFileSpout;

/**
 * Created by ap0n on 11/21/14.
 */
public class Topology {

  private final static int SPOUTS_COUNT = 1;
  private final static int COUNTER_BOLTS_COUNT = 1;
  private final static int FILTER_BOLTS_COUNT = 1;
  private final static int WORKERS_COUNT = 1;

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    // *********************************************************************************************
    // Spout
    // *********************************************************************************************
      builder.setSpout("wordsSpout", new ReadFromFileSpout(), SPOUTS_COUNT);

    // *********************************************************************************************
    // Bolts
    // *********************************************************************************************
    builder.setBolt("counterBolt", new CounterBolt(), COUNTER_BOLTS_COUNT)
        .fieldsGrouping("wordsSpout", "itemSetsStream", new Fields("itemSet"))
        .allGrouping("wordsSpout", "eofStream");
    builder.setBolt("filterBolt", new FilterBolt(), FILTER_BOLTS_COUNT)
        .shuffleGrouping("counterBolt", "resultStream")
        .allGrouping("counterBolt", "eofStream")
        .allGrouping("wordsSpout", "nStream");

    // *********************************************************************************************
    // Configuration
    // *********************************************************************************************
    Config config = new Config();
    config.setDebug(false);

    config.put("inputFilePath", "the_dataset");
    config.put("support", 0.01 /** 0.01*/);  // support %
    config.put("epsilon", 0.001 /** 0.01*/);  // epsilon %
    config.put("usePowerset", false);
    config.setNumWorkers(WORKERS_COUNT);

    // *********************************************************************************************
    // Deployment
    // *********************************************************************************************
    if (args != null && args.length > 0) {
      StormSubmitter.submitTopology(args[0], config, builder.createTopology());

    } else {
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("topology", config, builder.createTopology());
    }
  }
}
