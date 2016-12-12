package org.apache.solr.metrics;

import java.util.ArrayList;
import java.util.List;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Reservoir;

/**
 *
 */
public class LinkedHistogram extends Histogram implements LinkedMetric {
  private final List<Histogram> linked = new ArrayList<>(1);

  public LinkedHistogram(List<Histogram> linked) {
    this(new ExponentiallyDecayingReservoir(), linked);
  }

  public LinkedHistogram(Reservoir reservoir, List<Histogram> linked) {
    super(reservoir);
    if (linked != null) {
      this.linked.addAll(linked);
    }
  }

  public List<Histogram> getLinked() {
    return linked;
  }

  @Override
  public void update(long value) {
    super.update(value);
    for (Histogram h : linked) {
      h.update(value);
    }
  }
}
