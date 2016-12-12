package org.apache.solr.metrics;

import java.util.List;

import com.codahale.metrics.Metric;

/**
 *
 */
public interface LinkedMetric {

  List<? extends Metric> getLinked();
}
