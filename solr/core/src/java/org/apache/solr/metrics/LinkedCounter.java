package org.apache.solr.metrics;

import java.util.ArrayList;
import java.util.List;

import com.codahale.metrics.Counter;

/**
 *
 */
public class LinkedCounter extends Counter implements LinkedMetric {
  private final List<Counter> linked = new ArrayList<>(1);

  public LinkedCounter(List<Counter> linked) {
    super();
    if (linked != null) {
      this.linked.addAll(linked);
    }
  }

  public List<Counter> getLinked() {
    return linked;
  }

  @Override
  public void inc(long n) {
    super.inc(n);
    for (Counter c : linked) {
      c.inc(n);
    }
  }

  @Override
  public void dec(long n) {
    super.dec(n);
    for (Counter c : linked) {
      c.dec(n);
    }
  }
}
