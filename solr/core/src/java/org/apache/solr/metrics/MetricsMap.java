package org.apache.solr.metrics;

import java.util.Map;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;

/**
 * Dynamically constructed map of metrics, intentionally different from {@link com.codahale.metrics.MetricSet}
 * where each metric had to be known in advance and registered separately in {@link com.codahale.metrics.MetricRegistry}.
 * <p>Note: this awkwardly extends {@link Gauge} and not {@link Metric} because awkwardly {@link Metric} instances
 * are not supported by {@link com.codahale.metrics.MetricRegistryListener} :(</p>
 */
public interface MetricsMap extends Gauge<Map<String, Metric>> {

  Map<String, Metric> getValue(boolean detailed);

  default Map<String, Metric> getValue() {
    return getValue(false);
  }
}