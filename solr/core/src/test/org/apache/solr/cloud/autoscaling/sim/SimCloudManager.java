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
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.DistributedQueueFactory;
import org.apache.solr.client.solrj.cloud.autoscaling.DistribStateManager;
import org.apache.solr.client.solrj.cloud.autoscaling.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.autoscaling.SolrCloudManager;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.autoscaling.OverseerTriggerThread;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.rule.ImplicitSnitch;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simulated {@link SolrCloudManager}.
 */
public class SimCloudManager implements SolrCloudManager {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final SimDistribStateManager stateManager;
  private final SimClusterStateProvider clusterStateProvider;
  private final SimNodeStateProvider nodeStateProvider;
  private final Set<String> liveNodes = ConcurrentHashMap.newKeySet();
  private final SimDistributedQueueFactory queueFactory;
  private SolrClient solrClient;
  private final SimHttpServer httpServer;

  private final List<SolrInputDocument> systemColl = Collections.synchronizedList(new ArrayList<>());

  private Overseer.OverseerThread triggerThread;

  private static int nodeIdPort = 10000;

  public SimCloudManager() {
    this.stateManager = new SimDistribStateManager();
    this.clusterStateProvider = new SimClusterStateProvider(liveNodes);
    this.nodeStateProvider = new SimNodeStateProvider(liveNodes, this.clusterStateProvider, null);
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

  public static SimCloudManager createCluster(int numNodes) throws Exception {
    SimCloudManager cloudManager = new SimCloudManager();
    for (int i = 1; i <= numNodes; i++) {
      Map<String, Object> values = createNodeValues();
      String nodeId = (String)values.get(ImplicitSnitch.NODE);
      cloudManager.getSimClusterStateProvider().simAddNode(nodeId);
      cloudManager.getSimNodeStateProvider().simSetNodeValues(nodeId, values);
    }
    return cloudManager;
  }

  public static Map<String, Object> createNodeValues() {
    String host = "127.0.0.1";
    String port = "" + nodeIdPort++;
    String nodeId = host + ":" + port + "_solr";
    Map<String, Object> values = new HashMap<>();
    values.put("ip_1", "127");
    values.put("ip_2", "0");
    values.put("ip_3", "0");
    values.put("ip_4", "1");
    values.put(ImplicitSnitch.HOST, host);
    values.put(ImplicitSnitch.PORT, port);
    values.put(ImplicitSnitch.NODE, nodeId);
    values.put(ImplicitSnitch.CORES, 4);
    values.put(ImplicitSnitch.DISK, 123450000);
    values.put(ImplicitSnitch.SYSLOADAVG, 1.0);
    values.put(ImplicitSnitch.HEAPUSAGE, 123450000);
    return values;
  }

  public void simAddNode() throws Exception {
    Map<String, Object> values = createNodeValues();
    String nodeId = (String)values.get(ImplicitSnitch.NODE);
    clusterStateProvider.simAddNode(nodeId);
    nodeStateProvider.simSetNodeValues(nodeId, values);
  }

  public void simRemoveNode(String nodeId) throws Exception {
    clusterStateProvider.simRemoveNode(nodeId);
    nodeStateProvider.simRemoveNodeValues(nodeId);
  }

  public void simClearSystemCollection() {
    systemColl.clear();
  }

  public List<SolrInputDocument> simGetSystemCollection() {
    return systemColl;
  }


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
      return simHandleSolrRequest(req);
    }
  }

  public SolrResponse simHandleSolrRequest(SolrRequest req) throws IOException {
    LOG.info("--- got SolrRequest: " + req);
    if (req instanceof UpdateRequest) {
      // support only updates to the system collection
      UpdateRequest ureq = (UpdateRequest)req;
      if (ureq.getCollection() == null || !ureq.getCollection().equals(CollectionAdminParams.SYSTEM_COLL)) {
        throw new UnsupportedOperationException("Only .system updates are supported but got: " + req);
      }
      List<SolrInputDocument> docs = ureq.getDocuments();
      if (docs != null) {
        systemColl.addAll(docs);
      }
      return new UpdateResponse();
    }
    // support only a specific subset of collection admin ops
    if (!(req instanceof CollectionAdminRequest)) {
      throw new UnsupportedOperationException("Only CollectionAdminRequest-s are supported: " + req.getClass().getName());
    }
    SolrParams params = req.getParams();
    String a = params.get(CoreAdminParams.ACTION);
    SolrResponse rsp = new SolrResponseBase();
    if (a != null) {
      CollectionParams.CollectionAction action = CollectionParams.CollectionAction.get(a);
      if (action == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown action: " + a);
      }
      LOG.info("Invoking Collection Action :{} with params {}", action.toLower(), req.getParams().toQueryString());
      switch (action) {
        case ADDREPLICA:
          break;
        case ADDREPLICAPROP:
          break;
        case CREATE:
          break;
        case CLUSTERPROP:
          break;
        case CREATESHARD:
          break;
        case DELETE:
          break;
        case DELETENODE:
          break;
        case DELETEREPLICA:
          break;
        case DELETEREPLICAPROP:
          break;
        case DELETESHARD:
          break;
        case FORCELEADER:
          break;
        case LIST:
          NamedList results = new NamedList();
          results.add("collections", clusterStateProvider.simListCollections());
          rsp.setResponse(results);
          break;
        case MODIFYCOLLECTION:
          break;
        case MOVEREPLICA:
          break;
        case REBALANCELEADERS:
          break;
        case RELOAD:
          break;
        case REPLACENODE:
          break;
        case SPLITSHARD:
          break;
        default:
          throw new UnsupportedOperationException("Unsupported collection admin action=" + action);
      }
    } else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "action is a required param");
    }
    return rsp;

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
