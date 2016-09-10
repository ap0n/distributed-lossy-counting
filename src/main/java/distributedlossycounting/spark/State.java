package distributedlossycounting.spark;

import java.util.HashMap;

/**
 * Created by ap0n on 1/20/15.
 */
public class State {
  int n;  // The size of the stream seen so far
  HashMap<String, Integer> counters;  // lossy counting counters
}
