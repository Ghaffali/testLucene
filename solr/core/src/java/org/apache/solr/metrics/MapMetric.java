package org.apache.solr.metrics;

import java.util.Map;

import com.codahale.metrics.Metric;

/**
 * Dynamically constructed map of metrics, intentionally different from {@link com.codahale.metrics.MetricSet}
 * where each metric had to be known in advance and registered separately in {@link com.codahale.metrics.MetricRegistry}.
 */
public interface MapMetric extends Metric {

  Map<String, Metric> getMetrics(boolean detailed);

  default Map<String, Metric> getMetrics() {
    return getMetrics(false);
  }
}