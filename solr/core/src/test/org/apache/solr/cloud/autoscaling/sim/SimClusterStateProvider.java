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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SimClusterStateProvider implements ClusterStateProvider {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<String, List<ReplicaInfo>> nodeReplicaMap = new ConcurrentHashMap<>();
  private final Set<String> liveNodes;

  private final Map<String, Object> clusterProperties = new ConcurrentHashMap<>();
  private final Map<String, Map<String, Object>> collProperties = new ConcurrentHashMap<>();
  private final Map<String, Map<String, Map<String, Object>>> sliceProperties = new ConcurrentHashMap<>();

  private final ExecutorService simStateProviderPool;

  private final ReentrantLock lock = new ReentrantLock();

  /**
   * Zero-arg constructor. The instance needs to be initialized using the <code>sim*</code> methods in order
   * to ensure proper behavior, otherwise it will behave as a cluster with zero live nodes and zero replicas.
   */
  public SimClusterStateProvider(Set<String> liveNodes) {
    this.liveNodes = liveNodes;
    simStateProviderPool = ExecutorUtil.newMDCAwareCachedThreadPool(new DefaultSolrThreadFactory("simClusterStateProviderPool"));
  }

  // ============== SIMULATOR SETUP METHODS ====================

  public void simSetClusterState(ClusterState initialState) {
    lock.lock();
    try {
      collProperties.clear();
      sliceProperties.clear();
      nodeReplicaMap.clear();
      liveNodes.addAll(initialState.getLiveNodes());
      initialState.forEachCollection(dc -> {
        collProperties.computeIfAbsent(dc.getName(), name -> new HashMap<>()).putAll(dc.getProperties());
        dc.getSlices().forEach(s -> {
          sliceProperties.computeIfAbsent(dc.getName(), name -> new HashMap<>())
              .computeIfAbsent(s.getName(), name -> new HashMap<>()).putAll(s.getProperties());
          s.getReplicas().forEach(r -> {
            ReplicaInfo ri = new ReplicaInfo(r.getName(), r.getCoreName(), dc.getName(), s.getName(), r.getType(), r.getNodeName(), r.getProperties());
            if (liveNodes.contains(r.getNodeName())) {
              nodeReplicaMap.computeIfAbsent(r.getNodeName(), rn -> new ArrayList<>()).add(ri);
            }
          });
        });
      });
    } finally {
      lock.unlock();
    }
  }

  // todo: maybe hook up DistribStateManager /live_nodes ?
  // todo: maybe hook up DistribStateManager /clusterstate.json watchers?
  public boolean simAddNode(String nodeId) throws Exception {
    if (liveNodes.contains(nodeId)) {
      throw new Exception("Node " + nodeId + " already exists");
    }
    liveNodes.add(nodeId);
    return nodeReplicaMap.putIfAbsent(nodeId, new ArrayList<>()) == null;
  }

  // todo: maybe hook up DistribStateManager /live_nodes ?
  // todo: maybe hook up DistribStateManager /clusterstate.json watchers?
  public boolean simRemoveNode(String nodeId) {
    lock.lock();
    try {
      // mark every replica on that node as down
      List<ReplicaInfo> replicas = nodeReplicaMap.get(nodeId);
      if (replicas != null) {
        replicas.forEach(r -> {
          r.getVariables().put(ZkStateReader.STATE_PROP, Replica.State.DOWN.toString());
        });
      }
      boolean res = liveNodes.remove(nodeId);
      simStateProviderPool.submit(() -> simRunLeaderElection());
      return res;
    } finally {
      lock.unlock();
    }
  }

  // todo: maybe hook up DistribStateManager /clusterstate.json watchers?
  public void simAddReplica(String nodeId, ReplicaInfo replicaInfo) throws Exception {
    // make sure coreNodeName is unique across cluster
    for (Map.Entry<String, List<ReplicaInfo>> e : nodeReplicaMap.entrySet()) {
      for (ReplicaInfo ri : e.getValue()) {
        if (ri.getCore().equals(replicaInfo.getCore())) {
          throw new Exception("Duplicate coreNodeName for existing=" + ri + " on node " + e.getKey() + " and new=" + replicaInfo);
        }
      }
    }
    lock.lock();
    try {
      List<ReplicaInfo> replicas = nodeReplicaMap.computeIfAbsent(nodeId, n -> new ArrayList<>());
      replicas.add(replicaInfo);
      simStateProviderPool.submit(() -> simRunLeaderElection());
    } finally {
      lock.unlock();
    }
  }

  // todo: maybe hook up DistribStateManager /clusterstate.json watchers?
  public void simRemoveReplica(String nodeId, String coreNodeName) throws Exception {
    List<ReplicaInfo> replicas = nodeReplicaMap.computeIfAbsent(nodeId, n -> new ArrayList<>());
    lock.lock();
    try {
      for (int i = 0; i < replicas.size(); i++) {
        if (coreNodeName.equals(replicas.get(i).getCore())) {
          replicas.remove(i);
          simStateProviderPool.submit(() -> simRunLeaderElection());
          return;
        }
      }
      throw new Exception("Replica " + coreNodeName + " not found on node " + nodeId);
    } finally {
      lock.unlock();
    }
  }

  public synchronized void simRunLeaderElection() {
    try {
      ClusterState state = getClusterState();
      state.forEachCollection(dc -> {
        dc.getSlices().forEach(s -> {
          Replica leader = s.getLeader();
          if (leader == null || !liveNodes.contains(leader.getNodeName())) {
            LOG.info("Running leader election for " + dc.getName() + " / " + s.getName());
            if (s.getReplicas().isEmpty()) { // no replicas - punt
              return;
            }
            // mark all replicas as non-leader (probably not necessary) and collect all active and live
            List<ReplicaInfo> active = new ArrayList<>();
            s.getReplicas().forEach(r -> {
              AtomicReference<ReplicaInfo> riRef = new AtomicReference<>();
              // find our ReplicaInfo for this replica
              nodeReplicaMap.get(r.getNodeName()).forEach(info -> {
                if (info.getName().equals(r.getName())) {
                  riRef.set(info);
                }
              });
              ReplicaInfo ri = riRef.get();
              if (ri == null) {
                throw new RuntimeException("-- could not find ReplicaInfo for replica " + r);
              }
              ri.getVariables().remove(ZkStateReader.LEADER_PROP);
              if (r.isActive(liveNodes)) {
                active.add(ri);
              } else { // if it's on a node that is not live mark it down
                if (!liveNodes.contains(r.getNodeName())) {
                  ri.getVariables().put(ZkStateReader.STATE_PROP, Replica.State.DOWN.toString());
                }
              }
            });
            if (active.isEmpty()) {
              LOG.warn("-- can't find any active replicas for " + dc.getName() + " / " + s.getName());
              return;
            }
            // pick random one
            Collections.shuffle(active);
            ReplicaInfo ri = active.get(0);
            ri.getVariables().put(ZkStateReader.LEADER_PROP, "true");
            LOG.info("-- elected new leader for " + dc.getName() + " / " + s.getName() + ": " + ri);
          }
        });
      });
    } catch (Exception e) {
      throw new RuntimeException("simRunLeaderElection failed!", e);
    }
  }

  public void simDeleteCollection(String collection) throws Exception {
    lock.lock();
    try {
      collProperties.remove(collection);
      sliceProperties.remove(collection);
      nodeReplicaMap.forEach((n, replicas) -> {
        for (Iterator<ReplicaInfo> it = replicas.iterator(); it.hasNext(); ) {
          ReplicaInfo ri = it.next();
          if (ri.getCollection().equals(collection)) {
            it.remove();
          }
        }
      });
    } finally {
      lock.unlock();
    }
  }

  public void simDeleteAllCollections() {
    lock.lock();
    try {
      nodeReplicaMap.clear();
      collProperties.clear();
      sliceProperties.clear();
    } finally {
      lock.unlock();
    }
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
      lock.lock();
      try {
        Map<String, Object> props = collProperties.computeIfAbsent(coll, c -> new HashMap<>());
        props.clear();
        props.putAll(properties);
      } finally {
        lock.unlock();
      }
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
    lock.lock();
    try {
      sliceProps.clear();
      if (properties != null) {
        sliceProps.putAll(properties);
      }
    } finally {
      lock.unlock();
    }
  }

  public List<ReplicaInfo> simGetReplicaInfos(String node) {
    return nodeReplicaMap.get(node);
  }

  public List<String> simListCollections() {
    final Set<String> collections = new HashSet<>();
    lock.lock();
    try {
      nodeReplicaMap.forEach((n, replicas) -> {
        replicas.forEach(ri -> collections.add(ri.getCollection()));
      });
      return new ArrayList<>(collections);
    } finally {
      lock.unlock();
    }
  }

  // interface methods

  @Override
  public ClusterState.CollectionRef getState(String collection) {
    try {
      return getClusterState().getCollectionRef(collection);
    } catch (IOException e) {
      return null;
    }
  }

  @Override
  public Set<String> getLiveNodes() {
    return liveNodes;
  }

  @Override
  public List<String> resolveAlias(String alias) {
    throw new UnsupportedOperationException("resolveAlias not implemented");
  }

  @Override
  public ClusterState getClusterState() throws IOException {
    return new ClusterState(0, liveNodes, getCollectionStates());
  }

  private Map<String, DocCollection> getCollectionStates() {
    lock.lock();
    try {
      Map<String, Map<String, Map<String, Replica>>> collMap = new HashMap<>();
      nodeReplicaMap.forEach((n, replicas) -> {
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
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Map<String, Object> getClusterProperties() {
    return clusterProperties;
  }

  @Override
  public String getPolicyNameByCollection(String coll) {
    Map<String, Object> props = collProperties.computeIfAbsent(coll, c -> new HashMap<>());
    return (String)props.get("policy");
  }

  @Override
  public void connect() {

  }

  @Override
  public void close() throws IOException {
    simStateProviderPool.shutdownNow();
  }
}
