package org.apache.solr.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Timer;

/**
 *
 */
public class LinkedTimer extends Timer implements LinkedMetric {
  private final List<Timer> linked = new ArrayList<>(1);

  public LinkedTimer(List<Timer> linked) {
    super();
    if (linked != null) {
      this.linked.addAll(linked);
    }
  }

  public List<Timer> getLinked() {
    return linked;
  }

  @Override
  public <T> T time(Callable<T> event) throws Exception {
    throw new UnsupportedOperationException("Not supported in " + getClass().getName());
  }

  @Override
  public void update(long duration, TimeUnit unit) {
    super.update(duration, unit);
    for (Timer t : linked) {
      t.update(duration, unit);
    }
  }
}
