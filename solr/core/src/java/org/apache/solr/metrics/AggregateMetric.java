package org.apache.solr.metrics;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.codahale.metrics.Metric;

/**
 * This class is used for keeping several partial named values and providing useful statistics over them.
 */
public class AggregateMetric implements Metric {

  /**
   * Simple class to represent current value and how many times it was set.
   */
  public static class Update {
    public Number value;
    public final AtomicInteger updateCount = new AtomicInteger();

    public Update(Number value) {
      update(value);
    }

    public void update(Number value) {
      this.value = value;
      updateCount.incrementAndGet();
    }

    @Override
    public String toString() {
      return "Update{" +
          "value=" + value +
          ", updateCount=" + updateCount +
          '}';
    }
  }

  private final Map<String, Update> values = new ConcurrentHashMap<>();

  public void set(String name, Number value) {
    final Update existing = values.get(name);
    if (existing == null) {
      final Update created = new Update(value);
      final Update raced = values.putIfAbsent(name, created);
      if (raced != null) {
        raced.update(value);
      }
    } else {
      existing.update(value);
    }
  }

  public void clear(String name) {
    values.remove(name);
  }

  public void clear() {
    values.clear();
  }

  public int size() {
    return values.size();
  }

  public boolean isEmpty() {
    return values.isEmpty();
  }

  public Map<String, Update> getValues() {
    return Collections.unmodifiableMap(values);
  }

  // --------- stats ---------
  public double getMax() {
    if (values.isEmpty()) {
      return 0;
    }
    Double res = null;
    for (Update u : values.values()) {
      if (res == null) {
        res = u.value.doubleValue();
        continue;
      }
      if (u.value.doubleValue() > res) {
        res = u.value.doubleValue();
      }
    }
    return res;
  }

  public double getMin() {
    if (values.isEmpty()) {
      return 0;
    }
    Double res = null;
    for (Update u : values.values()) {
      if (res == null) {
        res = u.value.doubleValue();
        continue;
      }
      if (u.value.doubleValue() < res) {
        res = u.value.doubleValue();
      }
    }
    return res;
  }

  public double getMean() {
    if (values.isEmpty()) {
      return 0;
    }
    double total = 0;
    for (Update u : values.values()) {
      total += u.value.doubleValue();
    }
    return total / values.size();
  }

  public double getStdDev() {
    int size = values.size();
    if (size < 2) {
      return 0;
    }
    final double mean = getMean();
    double sum = 0;
    for (Update u : values.values()) {
      final double diff = u.value.doubleValue() - mean;
      sum += diff * diff;
    }
    final double variance = sum / (size - 1);
    return Math.sqrt(variance);
  }

  @Override
  public String toString() {
    return "AggregateMetric{" +
        "size=" + size() +
        ", max=" + getMax() +
        ", min=" + getMin() +
        ", stddev=" + getStdDev() +
        ", values=" + values +
        '}';
  }
}
