package org.apache.solr.metrics;

import java.util.Map;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;

/**
 * Dynamically constructed map of metrics, intentionally different from {@link com.codahale.metrics.MetricSet}
 * where each metric had to be known in advance and registered separately in {@link com.codahale.metrics.MetricRegistry}.
 * <p>Note: this awkwardly extends {@link Gauge} and not {@link Metric} because awkwardly {@link Metric} instances
 * are not supported by {@link com.codahale.metrics.MetricRegistryListener} :(</p>
 * <p>Note 2: values added to this metric map have to belong to the list of types supported by JMX:
 * {@link javax.management.openmbean.OpenType#ALLOWED_CLASSNAMES_LIST}, otherwise they will show up as
 * "Unavailable" in JConsole.</p>
 */
public interface MetricsMap extends Gauge<Map<String, Object>> {

  Map<String, Object> getValue(boolean detailed);

  default Map<String, Object> getValue() {
    return getValue(true);
  }
}