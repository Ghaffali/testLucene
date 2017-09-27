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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.ClusterDataProvider;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SimClusterDataProvider implements ClusterDataProvider {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<String, List<ReplicaInfo>> nodes = new ConcurrentHashMap<>();
  private final Map<String, Map<String, Object>> nodeValues = new ConcurrentHashMap<>();

  private final List<Watcher> autoscalingWatchers = new ArrayList<>();
  private final Map<String, Object> clusterProperties = new ConcurrentHashMap<>();
  private final Map<String, Map<String, Object>> collProperties = new ConcurrentHashMap<>();
  private final Map<String, Map<String, Map<String, Object>>> sliceProperties = new ConcurrentHashMap<>();

  private AutoScalingConfig autoScalingConfig = new AutoScalingConfig(Collections.emptyMap());
  private SimDistribStateManager distribStateManager;

  /**
   * Zero-arg constructor. The instance needs to be initialized using the <code>sim*</code> methods in order
   * to ensure proper behavior, otherwise it will behave as a cluster with zero live nodes and zero replicas.
   */
  public SimClusterDataProvider() {

  }

  /**
   * Construct simulated instance using initial cluster data.
   * @param initialState cluster state
   * @param autoScalingConfig autoscaling config
   * @param nodeValues per-node values (eg. metrics)
   */
  public SimClusterDataProvider(ClusterState initialState, AutoScalingConfig autoScalingConfig, Map<String, Map<String, Object>> nodeValues) {
    if (nodeValues != null) {
      this.nodeValues.putAll(nodeValues);
    }
    if (autoScalingConfig != null) {
      this.autoScalingConfig = (AutoScalingConfig)autoScalingConfig.clone();
    }
    simSetClusterState(initialState);
  }

  // ============== SIMULATOR SETUP METHODS ====================

  public void simSetClusterState(ClusterState initialState) {
    collProperties.clear();
    sliceProperties.clear();
    nodes.clear();
    Collection<String> liveNodes = initialState.getLiveNodes();
    initialState.forEachCollection(dc -> {
      collProperties.computeIfAbsent(dc.getName(), name -> new HashMap<>()).putAll(dc.getProperties());
      dc.getSlices().forEach(s -> {
        sliceProperties.computeIfAbsent(dc.getName(), name -> new HashMap<>())
            .computeIfAbsent(s.getName(), name -> new HashMap<>()).putAll(s.getProperties());
        s.getReplicas().forEach(r -> {
          ReplicaInfo ri = new ReplicaInfo(r.getName(), dc.getName(), s.getName(), r.getType(), r.getProperties());
          if (liveNodes.contains(r.getNodeName())) {
            nodes.computeIfAbsent(r.getNodeName(), rn -> new ArrayList<>()).add(ri);
          }
        });
      });
    });
    nodeValues.keySet().retainAll(liveNodes);
  }

  public void simSetDistribStateManager(SimDistribStateManager simDistribStateManager) {
    this.distribStateManager = simDistribStateManager;
  }

  public void simAddNodes(Collection<String> nodeIds) throws Exception {
    for (String node : nodeIds) {
      if (nodes.containsKey(node)) {
        throw new Exception("Node " + node + " already exists");
      }
    }
    for (String node : nodeIds) {
      simAddNode(node);
    }
  }

  // todo: maybe hook up DistribStateManager /live_nodes ?
  // todo: maybe hook up DistribStateManager /clusterstate.json watchers?
  public boolean simAddNode(String nodeId) throws Exception {
    if (nodes.containsKey(nodeId)) {
      throw new Exception("Node " + nodeId + " already exists");
    }
    return nodes.putIfAbsent(nodeId, new ArrayList<>()) == null;
  }

  // todo: maybe hook up DistribStateManager /live_nodes ?
  // todo: maybe hook up DistribStateManager /clusterstate.json watchers?
  public boolean simRemoveNode(String nodeId) {
    nodes.remove(nodeId);
    return nodeValues.remove(nodeId) != null;
  }

  // todo: maybe hook up DistribStateManager /clusterstate.json watchers?
  public void simAddReplica(String nodeId, ReplicaInfo replicaInfo) throws Exception {
    // make sure coreNodeName is unique across cluster
    for (Map.Entry<String, List<ReplicaInfo>> e : nodes.entrySet()) {
      for (ReplicaInfo ri : e.getValue()) {
        if (ri.getCore().equals(replicaInfo.getCore())) {
          throw new Exception("Duplicate coreNodeName for existing=" + ri + " on node " + e.getKey() + " and new=" + replicaInfo);
        }
      }
    }
    List<ReplicaInfo> replicas = nodes.computeIfAbsent(nodeId, n -> new ArrayList<>());
    replicas.add(replicaInfo);
  }

  // todo: maybe hook up DistribStateManager /clusterstate.json watchers?
  public void simRemoveReplica(String nodeId, String coreNodeName) throws Exception {
    List<ReplicaInfo> replicas = nodes.computeIfAbsent(nodeId, n -> new ArrayList<>());
    for (int i = 0; i < replicas.size(); i++) {
      if (coreNodeName.equals(replicas.get(i).getCore())) {
        replicas.remove(i);
        return;
      }
    }
    throw new Exception("Replica " + coreNodeName + " not found on node " + nodeId);
  }

  public void simSetNodeValues(String node, Map<String, Object> values) {
    nodeValues.put(node, new ConcurrentHashMap<>(values));
  }

  public void simSetNodeValue(String node, String key, Object value) {
    nodeValues.computeIfAbsent(node, n -> new ConcurrentHashMap<>()).put(key, value);
  }

  // todo: maybe hook up DistribStateManager /clusterstate.json watchers?
  public void simSetClusterProperties(Map<String, Object> properties) {
    clusterProperties.clear();
    if (properties != null) {
      this.clusterProperties.putAll(properties);
    }
  }

  public void simSetClusterProperty(String key, Object value) {
    if (value != null) {
      clusterProperties.put(key, value);
    } else {
      clusterProperties.remove(key);
    }
  }

  // todo: maybe hook up DistribStateManager /clusterstate.json watchers?
  public void simSetCollectionProperties(String coll, Map<String, Object> properties) {
    if (properties == null) {
      collProperties.remove(coll);
    } else {
      Map<String, Object> props = collProperties.computeIfAbsent(coll, c -> new HashMap<>());
      props.clear();
      props.putAll(properties);
    }
  }

  public void simSetCollectionProperty(String coll, String key, String value) {
    Map<String, Object> props = collProperties.computeIfAbsent(coll, c -> new HashMap<>());
    if (value == null) {
      props.remove(key);
    } else {
      props.put(key, value);
    }
  }

  public void setSliceProperties(String coll, String slice, Map<String, Object> properties) {
    Map<String, Object> sliceProps = sliceProperties.computeIfAbsent(coll, c -> new HashMap<>()).computeIfAbsent(slice, s -> new HashMap<>());
    sliceProps.clear();
    if (properties != null) {
      sliceProps.putAll(properties);
    }
  }

  // todo: maybe hook up DistribStateManager /autoscaling.json watchers?
  public void simSetAutoScalingConfig(AutoScalingConfig autoScalingConfig) {
    this.autoScalingConfig = autoScalingConfig;
    WatchedEvent ev = new WatchedEvent(Watcher.Event.EventType.NodeDataChanged, Watcher.Event.KeeperState.SyncConnected, ZkStateReader.SOLR_AUTOSCALING_CONF_PATH);
    for (Watcher w : autoscalingWatchers) {
      w.process(ev);
    }
    autoscalingWatchers.clear();
  }

  public SolrResponse simHandleSolrRequest(SolrRequest req) throws IOException {
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
      LOG.info("Invoked Collection Action :{} with params {}", action.toLower(), req.getParams().toQueryString());
    } else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "action is a required param");
    }

    return rsp;

  }

  // interface methods

  @Override
  public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
    Map<String, Object> values = nodeValues.get(node);
    if (values == null) {
      return Collections.emptyMap();
    }
    return values.entrySet().stream().filter(e -> tags.contains(e.getKey())).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
  }

  @Override
  public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
    List<ReplicaInfo> replicas = nodes.get(node);
    if (replicas == null) {
      return Collections.emptyMap();
    }
    Map<String, Map<String, List<ReplicaInfo>>> res = new HashMap<>();
    for (ReplicaInfo r : replicas) {
      Map<String, List<ReplicaInfo>> perCollection = res.computeIfAbsent(r.getCollection(), s -> new HashMap<>());
      List<ReplicaInfo> perShard = perCollection.computeIfAbsent(r.getShard(), s -> new ArrayList<>());
      perShard.add(r);
    }
    return res;
  }

  @Override
  public Collection<String> getLiveNodes() {
    return nodes.keySet();
  }

  @Override
  public ClusterState getClusterState() throws IOException {
    return new ClusterState(0, nodes.keySet(), getCollectionStates());
  }

  private Map<String, DocCollection> getCollectionStates() {
    Map<String, Map<String, Map<String, Replica>>> collMap = new HashMap<>();
    nodes.forEach((n, replicas) -> {
      replicas.forEach(ri -> {
        Map<String, Object> props = new HashMap<>(ri.getVariables());
        props.put(ZkStateReader.NODE_NAME_PROP, n);
        Replica r = new Replica(ri.getName(), props);
        collMap.computeIfAbsent(ri.getCollection(), c -> new HashMap<>())
            .computeIfAbsent(ri.getShard(), s -> new HashMap<>())
            .put(ri.getName(), r);
      });
    });
    Map<String, DocCollection> res = new HashMap<>();
    collMap.forEach((coll, shards) -> {
      Map<String, Slice> slices = new HashMap<>();
      shards.forEach((s, replicas) -> {
        Map<String, Object> sliceProps = sliceProperties.computeIfAbsent(coll, c -> new HashMap<>()).computeIfAbsent(s, sl -> new HashMap<>());
        Slice slice = new Slice(s, replicas, sliceProps);
        slices.put(s, slice);
      });
      Map<String, Object> collProps = collProperties.computeIfAbsent(coll, c -> new HashMap<>());
      DocCollection dc = new DocCollection(coll, slices, collProps, DocRouter.DEFAULT, 0, ZkStateReader.CLUSTER_STATE);
      res.put(coll, dc);
    });
    return res;
  }

  @Override
  public Map<String, Object> getClusterProperties() {
    return clusterProperties;
  }

  @Override
  public AutoScalingConfig getAutoScalingConfig(Watcher watcher) throws InterruptedException, IOException {
    if (watcher != null && !autoscalingWatchers.contains(watcher)) {
      autoscalingWatchers.add(watcher);
    }
    return autoScalingConfig;
  }

  @Override
  public String getPolicyNameByCollection(String coll) {
    Map<String, Object> props = collProperties.computeIfAbsent(coll, c -> new HashMap<>());
    return (String)props.get("policy");
  }
}
