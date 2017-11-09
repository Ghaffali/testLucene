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

package org.apache.solr.cloud.autoscaling.sim;

import java.io.IOException;
import java.util.Map;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.DistributedQueueFactory;
import org.apache.solr.client.solrj.cloud.autoscaling.DistribStateManager;
import org.apache.solr.client.solrj.cloud.autoscaling.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.autoscaling.SolrCloudManager;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.autoscaling.OverseerTriggerThread;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.core.SolrResourceLoader;

/**
 * Simulated {@link SolrCloudManager}.
 */
public class SimCloudManager implements SolrCloudManager {

  private final SimDistribStateManager stateManager;
  private final SimClusterStateProvider clusterStateProvider;
  private final SimNodeStateProvider nodeStateProvider;
  private final SimDistributedQueueFactory queueFactory;
  private SolrClient solrClient;
  private final SimHttpServer httpServer;

  private Overseer.OverseerThread triggerThread;

  public SimCloudManager() {
    this.stateManager = new SimDistribStateManager();
    this.clusterStateProvider = new SimClusterStateProvider();
    this.nodeStateProvider = new SimNodeStateProvider(this.clusterStateProvider, null);
    this.queueFactory = new SimDistributedQueueFactory();
    this.httpServer = new SimHttpServer();
    ThreadGroup triggerThreadGroup = new ThreadGroup("Simulated Overseer autoscaling triggers");
    OverseerTriggerThread trigger = new OverseerTriggerThread(new SolrResourceLoader(), this,
        new CloudConfig.CloudConfigBuilder("nonexistent", 0, "sim").build());
    triggerThread = new Overseer.OverseerThread(triggerThreadGroup, trigger, "Simulated OverseerAutoScalingTriggerThread");
    triggerThread.start();
  }

  public void setSolrClient(SolrClient solrClient) {
    this.solrClient = solrClient;
  }

  // ---------- simulator setup methods -----------


  // ---------- type-safe methods to obtain simulator components ----------
  public SimClusterStateProvider getSimClusterStateProvider() {
    return clusterStateProvider;
  }

  public SimNodeStateProvider getSimNodeStateProvider() {
    return nodeStateProvider;
  }

  public SimDistribStateManager getSimDistribStateManager() {
    return stateManager;
  }

  public SimDistributedQueueFactory getSimDistributedQueueFactory() {
    return queueFactory;
  }

  // --------- interface methods -----------

  @Override
  public ClusterStateProvider getClusterStateProvider() {
    return clusterStateProvider;
  }

  @Override
  public NodeStateProvider getNodeStateProvider() {
    return nodeStateProvider;
  }

  @Override
  public DistribStateManager getDistribStateManager() {
    return stateManager;
  }

  @Override
  public DistributedQueueFactory getDistributedQueueFactory() {
    return queueFactory;
  }

  @Override
  public SolrResponse request(SolrRequest req) throws IOException {
    if (solrClient != null) {
      try {
        return req.process(solrClient);
      } catch (SolrServerException e) {
        throw new IOException(e);
      }
    } else {
      return clusterStateProvider.simHandleSolrRequest(req);
    }
  }

  @Override
  public byte[] httpRequest(String url, SolrRequest.METHOD method, Map<String, String> headers, String payload, int timeout, boolean followRedirects) throws IOException {
    return httpServer.httpRequest(url, method, headers, payload, timeout, followRedirects);
  }

  @Override
  public void close() throws IOException {
    IOUtils.closeQuietly(clusterStateProvider);
    IOUtils.closeQuietly(nodeStateProvider);
    IOUtils.closeQuietly(stateManager);
    IOUtils.closeQuietly(triggerThread);
    triggerThread.interrupt();
  }
}
