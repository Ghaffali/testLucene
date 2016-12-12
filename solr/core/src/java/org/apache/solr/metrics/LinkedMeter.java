package org.apache.solr.metrics;

import java.util.ArrayList;
import java.util.List;

import com.codahale.metrics.Meter;

/**
 *
 */
public class LinkedMeter extends Meter implements LinkedMetric {
  private final List<Meter> linked = new ArrayList<>(1);

  public LinkedMeter(List<Meter> linked) {
    super();
    if (linked != null) {
      this.linked.addAll(linked);
    }
  }

  public List<Meter> getLinked() {
    return linked;
  }

  @Override
  public void mark(long n) {
    super.mark(n);
    for (Meter m : linked) {
      m.mark(n);
    }
  }
}
