/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.metrics;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;

/**
 *
 */
public class SolrMetricManager {

  public static final String REGISTRY_NAME_PREFIX = "solr.";

  // don't create instances of this class
  private SolrMetricManager() { }

  /**
   * An implementation of {@link MetricFilter} that selects metrics
   * with names that start with a prefix.
   */
  public static class PrefixFilter implements MetricFilter {
    private final String prefix;
    private final Set<String> matched = new HashSet<>();

    /**
     * Create a filter that uses the provided prefix.
     * @param prefix prefix to use, must not be null. If empty then any
     *               name will match.
     */
    public PrefixFilter(String prefix) {
      Preconditions.checkNotNull(prefix);
      this.prefix = prefix;
    }

    @Override
    public boolean matches(String name, Metric metric) {
      if (prefix.isEmpty()) {
        matched.add(name);
        return true;
      }
      if (name.startsWith(prefix)) {
        matched.add(name);
        return true;
      } else {
        return false;
      }
    }

    /**
     * Return the set of names that matched this filter.
     * @return
     */
    public Set<String> getMatched() {
      return Collections.unmodifiableSet(matched);
    }

    /**
     * Clear the set of names that matched.
     */
    public void reset() {
      matched.clear();
    }
  }

  /**
   * Return a set of existing registry names.
   */
  public static Set<String> registryNames() {
    return SharedMetricRegistries.names();
  }

  /**
   * Get (or create if not present) a named registry
   * @param registry name of the registry
   * @return existing or newly created registry
   */
  public static MetricRegistry registry(String registry) {
    return SharedMetricRegistries.getOrCreate(overridableRegistryName(registry));
  }

  /**
   * Remove a named registry.
   * @param registry name of the registry to remove
   */
  public static void removeRegistry(String registry) {
    SharedMetricRegistries.remove(registry);
  }

  /**
   * Remove all metrics from a specified registry.
   * @param registry registry name
   */
  public static void clearRegistry(String registry) {
    registry(registry).removeMatching(MetricFilter.ALL);
  }

  /**
   * Remove some metrics from a named registry
   * @param registry registry name
   * @param metricPath (optional) top-most metric name path elements. If empty then
   *        this is equivalent to calling {@link #clearRegistry(String)},
   *        otherwise non-empty elements will be joined using dotted notation
   *        to form a fully-qualified prefix. Metrics with names that start
   *        with the prefix will be removed.
   * @return set of metrics names that have been removed.
   */
  public static Set<String> clearMetrics(String registry, String... metricPath) {
    PrefixFilter filter;
    if (metricPath == null || metricPath.length == 0) {
      filter = new PrefixFilter("");
    } else {
      String prefix = MetricRegistry.name("", metricPath);
      filter = new PrefixFilter(prefix);
    }
    registry(registry).removeMatching(filter);
    return filter.getMatched();
  }

  /**
   * Create or get an existing named {@link Meter}
   * @param registry registry name
   * @param metricName metric name, either final name or a fully-qualified name
   *                   using dotted notation
   * @param metricPath (optional) additional top-most metric name path elements
   * @return existing or a newly created {@link Meter}
   */
  public static Meter meter(String registry, String metricName, String... metricPath) {
    return registry(registry).meter(mkName(metricName, metricPath));
  }

  /**
   * Create or get an existing named {@link Timer}
   * @param registry registry name
   * @param metricName metric name, either final name or a fully-qualified name
   *                   using dotted notation
   * @param metricPath (optional) additional top-most metric name path elements
   * @return existing or a newly created {@link Timer}
   */
  public static Timer timer(String registry, String metricName, String... metricPath) {
    return registry(registry).timer(mkName(metricName, metricPath));
  }

  /**
   * Create or get an existing named {@link Counter}
   * @param registry registry name
   * @param metricName metric name, either final name or a fully-qualified name
   *                   using dotted notation
   * @param metricPath (optional) additional top-most metric name path elements
   * @return existing or a newly created {@link Counter}
   */
  public static Counter counter(String registry, String metricName, String... metricPath) {
    return registry(registry).counter(mkName(metricName, metricPath));
  }

  /**
   * Create or get an existing named {@link Histogram}
   * @param registry registry name
   * @param metricName metric name, either final name or a fully-qualified name
   *                   using dotted notation
   * @param metricPath (optional) additional top-most metric name path elements
   * @return existing or a newly created {@link Histogram}
   */
  public static Histogram histogram(String registry, String metricName, String... metricPath) {
    return registry(registry).histogram(mkName(metricName, metricPath));
  }

  /**
   * This method creates a hierarchical name with arbitrary levels of hierarchy
   * @param name the final segment of the name, must not be null or empty.
   * @param path optional path segments, starting from the top level. Empty or null
   *             segments will be skipped.
   * @return fully-qualified name using dotted notation, with all valid hierarchy
   * segments prepended to the name.
   */
  public static String mkName(String name, String... path) {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("name must not be empty");
    }
    if (path == null || path.length == 0) {
      return name;
    } else {
      StringBuilder sb = new StringBuilder();
      for (String s : path) {
        if (s == null || s.isEmpty()) {
          continue;
        }
        if (sb.length() > 0) {
          sb.append('.');
        }
        sb.append(s);
      }
      if (sb.length() > 0) {
        sb.append('.');
      }
      sb.append(name);
      return sb.toString();
    }
  }

  /**
   * Allows named registries to be renamed using System properties.
   * This would be mostly be useful if you want to combine the metrics from a few registries for a single
   * reporter.
   * @param registry The name of the registry
   * @return A potentially overridden (via System properties) registry name
   */
  public static String overridableRegistryName(String registry) {
    String fqRegistry = enforcePrefix(registry);
    return enforcePrefix(System.getProperty(fqRegistry,fqRegistry));
  }

  /**
   * Enforces the leading {@link #REGISTRY_NAME_PREFIX} in a name.
   * @param name input name, possibly without the prefix
   * @return original name if it contained the prefix, or the
   * input name with the prefix prepended.
   */
  public static String enforcePrefix(String name) {
    if (name.startsWith(REGISTRY_NAME_PREFIX)) {
      return name;
    } else {
      return new StringBuilder(REGISTRY_NAME_PREFIX).append(name).toString();
    }
  }
}
