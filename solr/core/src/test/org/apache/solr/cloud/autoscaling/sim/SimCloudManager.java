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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

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
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.autoscaling.AutoScalingHandler;
import org.apache.solr.cloud.autoscaling.OverseerTriggerThread;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.rule.ImplicitSnitch;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectCache;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.OverseerCollectionMessageHandler.REQUESTID;

/**
 * Simulated {@link SolrCloudManager}.
 */
public class SimCloudManager implements SolrCloudManager {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final SimDistribStateManager stateManager;
  private final SimClusterStateProvider clusterStateProvider;
  private final SimNodeStateProvider nodeStateProvider;
  private final AutoScalingHandler autoScalingHandler;
  private final Set<String> liveNodes = ConcurrentHashMap.newKeySet();
  private final SimDistributedQueueFactory queueFactory;
  private final ObjectCache objectCache = new ObjectCache();
  private TimeSource timeSource;
  private SolrClient solrClient;
  private final SimHttpServer httpServer;

  private final List<SolrInputDocument> systemColl = Collections.synchronizedList(new ArrayList<>());
  private final ExecutorService simCloudManagerPool;


  private Overseer.OverseerThread triggerThread;

  private static int nodeIdPort = 10000;

  public SimCloudManager(TimeSource timeSource) throws Exception {
    this.stateManager = new SimDistribStateManager();
    // init common paths
    stateManager.makePath(ZkStateReader.CLUSTER_STATE);
    stateManager.makePath(ZkStateReader.CLUSTER_PROPS);
    stateManager.makePath(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH);
    stateManager.makePath(ZkStateReader.LIVE_NODES_ZKNODE);
    stateManager.makePath(ZkStateReader.ROLES);

    this.timeSource = timeSource != null ? timeSource : TimeSource.NANO_TIME;
    this.clusterStateProvider = new SimClusterStateProvider(liveNodes, this);
    this.nodeStateProvider = new SimNodeStateProvider(liveNodes, this.stateManager, this.clusterStateProvider, null);
    this.queueFactory = new SimDistributedQueueFactory();
    this.httpServer = new SimHttpServer();
    this.simCloudManagerPool = ExecutorUtil.newMDCAwareCachedThreadPool(new DefaultSolrThreadFactory("simCloudManagerPool"));
    this.autoScalingHandler = new AutoScalingHandler(this, new SolrResourceLoader());
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

  public static SimCloudManager createCluster(int numNodes, TimeSource timeSource) throws Exception {
    SimCloudManager cloudManager = new SimCloudManager(timeSource);
    for (int i = 1; i <= numNodes; i++) {
      Map<String, Object> values = createNodeValues(null);
      if (i == 1) { // designated Overseer
        //values.put(ImplicitSnitch.NODEROLE, "overseer");
      }
      String nodeId = (String)values.get(ImplicitSnitch.NODE);
      cloudManager.getSimClusterStateProvider().simAddNode(nodeId);
      cloudManager.getSimNodeStateProvider().simSetNodeValues(nodeId, values);
    }
    return cloudManager;
  }

  public static SimCloudManager createCluster(ClusterState initialState, TimeSource timeSource) throws Exception {
    SimCloudManager cloudManager = new SimCloudManager(timeSource);
    cloudManager.getSimClusterStateProvider().simSetClusterState(initialState);
    for (String node : cloudManager.getClusterStateProvider().getLiveNodes()) {
      cloudManager.getSimNodeStateProvider().simSetNodeValues(node, createNodeValues(node));
    }
    return cloudManager;
  }

  public static Map<String, Object> createNodeValues(String nodeName) {
    Map<String, Object> values = new HashMap<>();
    String host, nodeId;
    int port;
    if (nodeName == null) {
      host = "127.0.0.1";
      port = nodeIdPort++;
      nodeId = host + ":" + port + "_solr";
      values.put("ip_1", "127");
      values.put("ip_2", "0");
      values.put("ip_3", "0");
      values.put("ip_4", "1");
    } else {
      String[] hostPortCtx = nodeName.split(":");
      if (hostPortCtx.length != 2) {
        throw new RuntimeException("Invalid nodeName " + nodeName);
      }
      host = hostPortCtx[0];
      String[] portCtx = hostPortCtx[1].split("_");
      if (portCtx.length != 2) {
        throw new RuntimeException("Invalid port_context in nodeName " + nodeName);
      }
      port = Integer.parseInt(portCtx[0]);
      nodeId = host + ":" + port + "_" + portCtx[1];
      String[] ip = host.split("\\.");
      if (ip.length == 4) {
        values.put("ip_1", ip[0]);
        values.put("ip_2", ip[1]);
        values.put("ip_3", ip[2]);
        values.put("ip_4", ip[3]);
      }
    }
    values.put(ImplicitSnitch.HOST, host);
    values.put(ImplicitSnitch.PORT, port);
    values.put(ImplicitSnitch.NODE, nodeId);
    values.put(ImplicitSnitch.CORES, 0);
    values.put(ImplicitSnitch.DISK, 1000);
    values.put(ImplicitSnitch.SYSLOADAVG, 1.0);
    values.put(ImplicitSnitch.HEAPUSAGE, 123450000);
    values.put("INDEX.sizeInBytes", 12345000);
    values.put("sysprop.java.version", System.getProperty("java.version"));
    values.put("sysprop.java.vendor", System.getProperty("java.vendor"));
    // fake some metrics expected in tests
    values.put("metrics:solr.node:ADMIN./admin/authorization.clientErrors:count", 0);
    values.put("metrics:solr.jvm:buffers.direct.Count", 0);
    return values;
  }

  public String simAddNode() throws Exception {
    Map<String, Object> values = createNodeValues(null);
    String nodeId = (String)values.get(ImplicitSnitch.NODE);
    clusterStateProvider.simAddNode(nodeId);
    nodeStateProvider.simSetNodeValues(nodeId, values);
    return nodeId;
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

  public SolrClient simGetSolrClient() {
    if (solrClient != null) {
      return solrClient;
    } else {
      return new SolrClient() {
        @Override
        public NamedList<Object> request(SolrRequest request, String collection) throws SolrServerException, IOException {
          SolrResponse rsp = SimCloudManager.this.request(request);
          return rsp.getResponse();
        }

        @Override
        public void close() throws IOException {

        }
      };
    }
  }

  public <T> Future<T> submit(Callable<T> callable) {
    return simCloudManagerPool.submit(callable);
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
  public ObjectCache getObjectCache() {
    return objectCache;
  }

  @Override
  public TimeSource getTimeSource() {
    return timeSource;
  }

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
      try {
        Future<SolrResponse> res = submit(() -> simHandleSolrRequest(req));
        return res.get();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  public SolrResponse simHandleSolrRequest(SolrRequest req) throws IOException, InterruptedException {
    LOG.debug("--- got SolrRequest: " + req.getMethod() + " " + req.getPath() +
        (req.getParams() != null ? " " + req.getParams().toQueryString() : ""));
    if (req.getPath() != null && req.getPath().startsWith("/admin/autoscaling") ||
        req.getPath().startsWith("/cluster/autoscaling")) {
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(CommonParams.PATH, req.getPath());
      LocalSolrQueryRequest queryRequest = new LocalSolrQueryRequest(null, params);
      RequestWriter.ContentWriter cw = req.getContentWriter("application/json");
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      cw.write(baos);
      String payload = baos.toString("UTF-8");
      LOG.debug("-- payload: " + payload);
      queryRequest.setContentStreams(Collections.singletonList(new ContentStreamBase.StringStream(payload)));
      queryRequest.getContext().put("httpMethod", req.getMethod().toString());
      SolrQueryResponse queryResponse = new SolrQueryResponse();
      autoScalingHandler.handleRequest(queryRequest, queryResponse);
      if (queryResponse.getException() != null) {
        throw new IOException(queryResponse.getException());
      }
      SolrResponse rsp = new SolrResponseBase();
      rsp.setResponse(queryResponse.getValues());
      LOG.debug("-- response: " + rsp);
      return rsp;
    }
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
      throw new UnsupportedOperationException("Only some CollectionAdminRequest-s are supported: " + req.getClass().getName());
    }
    SolrParams params = req.getParams();
    String a = params.get(CoreAdminParams.ACTION);
    SolrResponse rsp = new SolrResponseBase();
    rsp.setResponse(new NamedList<>());
    if (a != null) {
      CollectionParams.CollectionAction action = CollectionParams.CollectionAction.get(a);
      if (action == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown action: " + a);
      }
      LOG.info("Invoking Collection Action :{} with params {}", action.toLower(), req.getParams().toQueryString());
      NamedList results = new NamedList();
      rsp.setResponse(results);
      switch (action) {
        case REQUESTSTATUS:
          // we complete all async ops immediately
          String requestId = req.getParams().get(REQUESTID);
          SimpleOrderedMap<String> status = new SimpleOrderedMap<>();
          status.add("state", RequestStatusState.COMPLETED.getKey());
          status.add("msg", "found [" + requestId + "] in completed tasks");
          results.add("status", status);
          results.add("success", "");
          // ExecutePlanAction expects a specific response class
          rsp = new CollectionAdminRequest.RequestStatusResponse();
          rsp.setResponse(results);
          break;
        case DELETESTATUS:
          requestId = req.getParams().get(REQUESTID);
          results.add("status", "successfully removed stored response for [" + requestId + "]");
          results.add("success", "");
          break;
        case CREATE:
          try {
            clusterStateProvider.simCreateCollection(new ZkNodeProps(req.getParams().toNamedList().asMap(10)), results);
          } catch (Exception e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
          }
          break;
        case DELETE:
          clusterStateProvider.simDeleteCollection(req.getParams().get(CommonParams.NAME),
              req.getParams().get(CommonAdminParams.ASYNC), results);
          break;
        case LIST:
          results.add("collections", clusterStateProvider.simListCollections());
          break;
        case MOVEREPLICA:
          try {
            clusterStateProvider.simMoveReplica(new ZkNodeProps(req.getParams().toNamedList().asMap(10)), results);
          } catch (Exception e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
          }
          break;
        case OVERSEERSTATUS:
          if (req.getParams().get(CommonAdminParams.ASYNC) != null) {
            results.add(REQUESTID, req.getParams().get(CommonAdminParams.ASYNC));
          }
          if (!liveNodes.isEmpty()) {
            results.add("leader", liveNodes.iterator().next());
          }
          results.add("overseer_queue_size", 0);
          results.add("overseer_work_queue_size", 0);
          results.add("overseer_collection_queue_size", 0);
          results.add("success", "");
          break;
        case ADDROLE:
          nodeStateProvider.simAddNodeValue(req.getParams().get("node"), "nodeRole", req.getParams().get("role"));
          break;
        case CREATESHARD:
          try {
            clusterStateProvider.simCreateShard(new ZkNodeProps(req.getParams().toNamedList().asMap(10)), results);
          } catch (Exception e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
          }
          break;
        default:
          throw new UnsupportedOperationException("Unsupported collection admin action=" + action + " in request: " + req.getParams());
      }
    } else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "action is a required param in request: " + req.getParams());
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
    IOUtils.closeQuietly(objectCache);
    simCloudManagerPool.shutdownNow();
    triggerThread.interrupt();
  }
}
