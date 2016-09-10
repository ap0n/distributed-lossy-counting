package distributedlossycounting.storm.spouts;

/**
 * Created by ap0n on 11/21/14.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * Read lines from a file and emit words
 */
public class ReadFromFileSpout extends BaseRichSpout {

  SpoutOutputCollector collector;
  String filePath;
  BufferedReader reader;
  boolean done;
  Logger logger = LoggerFactory.getLogger(ReadFromFileSpout.class);
  int thisTaskIndex;
  int tasksCount;
  boolean usePowerset;

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream("itemSetsStream", new Fields("itemSet"));
    outputFieldsDeclarer.declareStream("nStream", new Fields("setSize"));
    outputFieldsDeclarer.declareStream("eofStream", new Fields("eof"));
  }

  @Override
  public void open(Map config, TopologyContext topologyContext,
                   SpoutOutputCollector spoutOutputCollector) {
    collector = spoutOutputCollector;
    thisTaskIndex = topologyContext.getThisTaskIndex();
    tasksCount = topologyContext.getComponentTasks("wordsSpout").size();
    filePath = config.get("inputFilePath").toString();
    String taskFilePath;
    usePowerset = Boolean.parseBoolean(config.get("usePowerset").toString());

    if (tasksCount == 1) {
      taskFilePath = filePath + ".txt";
    } else {
      taskFilePath = filePath + (thisTaskIndex % tasksCount) + ".txt";
    }

    try {
      reader = new BufferedReader(new FileReader(taskFilePath));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    done = false;
  }

  @Override
  public void nextTuple() {
    if (done) {
      return;
    }
    try {
      String line = reader.readLine();
      if (line == null) {
        done = true;
        logger.info("Read every line" + thisTaskIndex);
        collector.emit("eofStream", new Values(""));
        return;
      }
      String[] items = line.split(" ");

      String[] sets;
      if (usePowerset) {
        sets = getPowerset(items).toArray(new String[0]);
      } else {
        sets = items;
      }

      for (String set : sets) {
        if (!set.isEmpty()) {
          collector.emit("itemSetsStream", new Values(set));
          int setSize = set.split(" ").length;
          collector.emit("nStream", new Values(setSize));
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static ArrayList<String> getPowerset(String[] set) {
    // Eliminate duplicate items
    String[] uniqueItems = new HashSet<String>(Arrays.asList(set)).toArray(new String[0]);

    return calculatePowerset(uniqueItems, uniqueItems.length, null);
  }

  private static ArrayList<String> calculatePowerset(String[] set, int setSize,
                                                     ArrayList<String> powerset) {

    if (setSize < 0) {
      return null;
    }

    if (setSize == 0) {
      if (powerset == null) {
        powerset = new ArrayList<String>();
      }
      powerset.add("");
      return powerset;
    }

    powerset = calculatePowerset(set, setSize - 1, powerset);

    ArrayList<String> tmp = new ArrayList<String>();
    for (String s : powerset) {
      if (s.equals("")) {
        tmp.add(set[setSize - 1]);
      } else {
        tmp.add(s + " " + set[setSize - 1]);
      }
    }
    powerset.addAll(tmp);
    return powerset;
  }
}
