package distributedlossycounting.storm.commons;

/**
 * Created by ap0n on 11/21/14.
 */
public class CounterValue {
  private int f;
  private int delta;

  public CounterValue(int f, int delta) {
    this.f = f;
    this.delta = delta;
  }

  public int getF() {
    return f;
  }

  public void setF(int f) {
    this.f = f;
  }

  public int getDelta() {
    return delta;
  }

  public void setDelta(int delta) {
    this.delta = delta;
  }
}
