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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.MetricRegistry;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.core.SolrResourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for collecting metrics from {@link SolrMetricProducer}'s
 * and exposing metrics to {@link SolrMetricReporter}'s.
 */
public class SolrCoreMetricManager implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final SolrCore core;

  /**
   * Constructs a metric manager.
   *
   * @param core the metric manager's core
   */
  public SolrCoreMetricManager(SolrCore core) {
    this.core = core;
  }

  /**
   * Load reporters configured globally and specific to {@link org.apache.solr.core.SolrInfoMBean.Group#core}
   * group or with a registry name specific to this core.
   */
  public void loadReporters() {
    NodeConfig nodeConfig = core.getCoreDescriptor().getCoreContainer().getConfig();
    PluginInfo[] pluginInfos = nodeConfig.getMetricReporterPlugins();
    SolrMetricManager.loadReporters(pluginInfos, core.getResourceLoader(), SolrInfoMBean.Group.core, core.getName());
  }

  /**
   * Make sure that metrics already collected that correspond to the old core name
   * are carried over and will be used under the new core name.
   * This method also reloads reporters so that they use the new core name.
   * @param oldCoreName core name before renaming, may be null
   * @param newCoreName core name after renaming, not null
   */
  public void afterCoreSetName(String oldCoreName, String newCoreName) {
    if (newCoreName.equals(oldCoreName)) {
      return;
    }
    String oldRegistryName = SolrMetricManager.getRegistryName(SolrInfoMBean.Group.core, oldCoreName);
    String newRegistryName = SolrMetricManager.getRegistryName(SolrInfoMBean.Group.core, newCoreName);
    // close old reporters
    SolrMetricManager.closeReporters(oldRegistryName);
    SolrMetricManager.moveMetrics(oldRegistryName, newRegistryName, null);
    // old registry is no longer used - we have moved the metrics
    SolrMetricManager.removeRegistry(oldRegistryName);
    // load reporters again, using the new core name
    loadReporters();
  }

  /**
   * Registers a mapping of name/metric's with the manager's metric registry.
   *
   * @param scope     the scope of the metrics to be registered (e.g. `/admin/ping`)
   * @param producer  producer of metrics to be registered
   */
  public void registerMetricProducer(String scope, SolrMetricProducer producer) {
    if (scope == null || producer == null || producer.getCategory() == null) {
      throw new IllegalArgumentException("registerMetricProducer() called with illegal arguments: " +
          "scope = " + scope + ", producer = " + producer);
    }
    Collection<String> registered = producer.initializeMetrics(getRegistryName(), getLinkedRegistries(), scope);
    if (registered == null || registered.isEmpty()) {
      throw new IllegalArgumentException("registerMetricProducer() did not register any metrics " +
      "for scope = " + scope + ", producer = " + producer);
    }
  }

  /**
   * Closes reporters specific to this core.
   */
  @Override
  public void close() throws IOException {
    SolrMetricManager.closeReporters(getRegistryName());
  }

  public SolrCore getCore() {
    return core;
  }

  /**
   * Retrieves the metric registry name of the manager.
   *
   * @return the metric registry name of the manager.
   */
  public String getRegistryName() {
    return SolrMetricManager.getRegistryName(SolrInfoMBean.Group.core, core.getName());
  }

  public List<String> getLinkedRegistries() {
    String collectionName = core.getCoreDescriptor().getCollectionName();
    if (collectionName == null) {
      return Collections.emptyList();
    } else {
      if (collectionName.equals(core.getName())) {
        return Collections.emptyList();
      } else {
        return Collections.singletonList(collectionName);
      }
    }
  }
}
