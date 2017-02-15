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
package org.apache.solr.metrics.reporters.solr;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.admin.MetricsCollectorHandler;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class reports selected metrics from replicas to shard leader.
 * <p>Example configuration:</p>
 * <pre>
 *    <reporter name="test" group="replica">
 *      <int name="period">11</int>
 *      <str name="filter">UPDATE\./update/.*requests</str>
 *      <str name="filter">QUERY\./select.*requests</str>
 *    </reporter>
 * </pre>
 */
public class SolrReplicaReporter extends SolrMetricReporter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final List<String> DEFAULT_FILTERS = new ArrayList(){{
    add("TLOG.*");
    add("REPLICATION.*");
    add("INDEX.flush.*");
    add("INDEX.merge.major.*");
    add("UPDATE\\./update/.*requests");
    add("QUERY\\./select.*requests");
  }};

  private String groupId;
  private String handler = MetricsCollectorHandler.HANDLER_PATH;
  private int period = 60;
  private List<String> filters = new ArrayList<>();

  private SolrReporter reporter;

  /**
   * Create a reporter for metrics managed in a named registry.
   *
   * @param metricManager
   * @param registryName  registry to use, one of registries managed by
   *                      {@link SolrMetricManager}
   */
  public SolrReplicaReporter(SolrMetricManager metricManager, String registryName) {
    super(metricManager, registryName);
  }

  public void setGroupId(String groupId) {
    this.groupId = groupId;
  }

  public void setHandler(String handler) {
    this.handler = handler;
  }

  public void setPeriod(int period) {
    this.period = period;
  }

  // for unit tests
  int getPeriod() {
    return period;
  }

  public void setFilter(List<String> filterConfig) {
    if (filterConfig == null || filterConfig.isEmpty()) {
      return;
    }
    filters = filterConfig;
  }

  @Override
  protected void validate() throws IllegalStateException {
    if (period < 1) {
      log.info("Turning off replica reporter, period=" + period);
    }
    if (filters.isEmpty()) {
      filters = DEFAULT_FILTERS;
    }
    // start in inform(...) only when core is available
  }

  @Override
  public void close() throws IOException {
    if (reporter != null) {
      reporter.close();
    }
  }

  public void setCore(SolrCore core) {
    if (reporter != null) {
      reporter.close();
    }
    if (core.getCoreDescriptor().getCloudDescriptor() == null) {
      // not a cloud core
      log.warn("Not initializing replica reporter for non-cloud core " + core.getName());
      return;
    }
    if (period < 1) { // don't start it
      return;
    }
    // our id is coreNodeName
    String id = core.getCoreDescriptor().getCloudDescriptor().getCoreNodeName();
    SolrReporter.Report spec = new SolrReporter.Report(groupId, null, registryName, filters);
    reporter = SolrReporter.Builder.forRegistries(metricManager, Collections.singletonList(spec))
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .withHandler(handler)
        .withReporterId(id)
        .cloudClient(false) // we want to send reports specifically to a selected leader instance
        .skipAggregateValues(true) // we don't want to transport details of aggregates
        .skipHistograms(true) // we don't want to transport histograms
        .build(core.getCoreDescriptor().getCoreContainer().getUpdateShardHandler().getHttpClient(), new LeaderUrlSupplier(core));

    reporter.start(period, TimeUnit.SECONDS);
  }

  private static class LeaderUrlSupplier implements Supplier<String> {
    private SolrCore core;

    LeaderUrlSupplier(SolrCore core) {
      this.core = core;
    }

    @Override
    public String get() {
      CloudDescriptor cd = core.getCoreDescriptor().getCloudDescriptor();
      if (cd == null) {
        return null;
      }
      ClusterState state = core.getCoreDescriptor().getCoreContainer().getZkController().getClusterState();
      DocCollection collection = state.getCollection(core.getCoreDescriptor().getCollectionName());
      Replica replica = collection.getLeader(core.getCoreDescriptor().getCloudDescriptor().getShardId());
      if (replica == null) {
        return null;
      }
      return replica.getStr("base_url");
    }
  }
}
