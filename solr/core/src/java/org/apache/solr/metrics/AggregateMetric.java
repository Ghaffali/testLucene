package org.apache.solr.metrics;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.Metric;

/**
 * This class is used for keeping several partial named values and providing useful statistics over them.
 */
public class AggregateMetric implements Metric {
  private final Map<String, Double> values = new ConcurrentHashMap<>();

  public void set(String name, double value) {
    values.put(name, value);
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

  public Map<String, Double> getValues() {
    return Collections.unmodifiableMap(values);
  }

  // --------- stats ---------
  public double getMax() {
    if (values.isEmpty()) {
      return 0;
    }
    Double res = null;
    for (Double d : values.values()) {
      if (res == null) {
        res = d;
        continue;
      }
      if (d > res) {
        res = d;
      }
    }
    return res;
  }

  public double getMin() {
    if (values.isEmpty()) {
      return 0;
    }
    Double res = null;
    for (Double d : values.values()) {
      if (res == null) {
        res = d;
        continue;
      }
      if (d < res) {
        res = d;
      }
    }
    return res;
  }

  public double getMean() {
    if (values.isEmpty()) {
      return 0;
    }
    double total = 0;
    for (Double d : values.values()) {
      total += d;
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
    for (Double d : values.values()) {
      final double diff = d - mean;
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
