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
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.codahale.metrics.MetricFilter;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricReporter;

/**
 * This class reports selected metrics from replicas to a shard leader.
 */
public class SolrReplicaReporter extends SolrMetricReporter {
  public static final String LEADER_REGISTRY = "leaderRegistry";

  public static final String[] METRICS = {
    "TLOG", "REPLICATION", "INDEX"
  };

  private String leaderRegistry;
  private String handler;
  private int period = 60;

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

  public void setLeaderRegistry(String leaderRegistry) {
    this.leaderRegistry = leaderRegistry;
  }

  public void setHandler(String handler) {
    this.handler = handler;
  }

  public void setPeriod(int period) {
    this.period = period;
  }

  @Override
  protected void validate() throws IllegalStateException {
    if (period < 1) {
      throw new IllegalStateException("Period must be greater than 0");
    }
    // start in inform(...) only when core is available
  }

  @Override
  public void close() throws IOException {
    if (reporter != null) {
      reporter.close();;
    }
  }

  public void setCore(SolrCore core) {
    if (reporter != null) {
      reporter.close();
    }
    // our id is nodeName
    String id = core.getCoreDescriptor().getCloudDescriptor().getCoreNodeName();
    MetricFilter filter = new SolrMetricManager.PrefixFilter(METRICS);
    reporter = SolrReporter.Builder.forRegistry(metricManager.registry(registryName))
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .withHandler(handler)
        .filter(filter)
        .withId(id)
        .cloudClient(false) // we want to send reports specifically to a selected leader instance
        .withGroup(leaderRegistry)
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
