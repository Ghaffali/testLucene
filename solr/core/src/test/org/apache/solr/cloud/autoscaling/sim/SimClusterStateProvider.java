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
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.solr.client.solrj.cloud.autoscaling.DistribStateManager;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.cloud.autoscaling.Suggestion;
import org.apache.solr.client.solrj.cloud.autoscaling.VersionedData;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.cloud.ActionThrottle;
import org.apache.solr.cloud.AddReplicaCmd;
import org.apache.solr.cloud.Assign;
import org.apache.solr.cloud.CreateCollectionCmd;
import org.apache.solr.cloud.CreateShardCmd;
import org.apache.solr.cloud.SplitShardCmd;
import org.apache.solr.cloud.overseer.ClusterStateMutator;
import org.apache.solr.cloud.overseer.CollectionMutator;
import org.apache.solr.cloud.overseer.ZkWriteCommand;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ReplicaPosition;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CommonParams.NAME;

/**
 * Simulated {@link ClusterStateProvider}.
 */
public class SimClusterStateProvider implements ClusterStateProvider {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<String, List<ReplicaInfo>> nodeReplicaMap = new ConcurrentHashMap<>();
  private final Set<String> liveNodes;
  private final DistribStateManager stateManager;
  private final SimCloudManager cloudManager;

  private final Map<String, Object> clusterProperties = new ConcurrentHashMap<>();
  private final Map<String, Map<String, Object>> collProperties = new ConcurrentHashMap<>();
  private final Map<String, Map<String, Map<String, Object>>> sliceProperties = new ConcurrentHashMap<>();

  private final ReentrantLock lock = new ReentrantLock();

  private final ActionThrottle leaderThrottle;

  // default map of: operation -> delay
  private final Map<String, Long> defaultOpDelays = new HashMap<>();
  // per-collection map of: collection -> op -> delay
  private final Map<String, Map<String, Long>> opDelays = new ConcurrentHashMap<>();


  private ClusterState lastSavedState = null;
  private Map<String, Object> lastSavedProperties = null;

  private AtomicReference<Map<String, DocCollection>> collectionsStatesRef = new AtomicReference<>();

  /**
   * Zero-arg constructor. The instance needs to be initialized using the <code>sim*</code> methods in order
   * to ensure proper behavior, otherwise it will behave as a cluster with zero live nodes and zero replicas.
   */
  public SimClusterStateProvider(Set<String> liveNodes, SimCloudManager cloudManager) {
    this.liveNodes = liveNodes;
    this.cloudManager = cloudManager;
    this.stateManager = cloudManager.getDistribStateManager();
    this.leaderThrottle = new ActionThrottle("leader", 5000, cloudManager.getTimeSource());
    // names are CollectionAction names, delays are in ms (simulated time)
    defaultOpDelays.put(CollectionParams.CollectionAction.MOVEREPLICA.name(), 5000L);
    defaultOpDelays.put(CollectionParams.CollectionAction.DELETEREPLICA.name(), 5000L);
    defaultOpDelays.put(CollectionParams.CollectionAction.ADDREPLICA.name(), 500L);
    defaultOpDelays.put(CollectionParams.CollectionAction.SPLITSHARD.name(), 5000L);
    defaultOpDelays.put(CollectionParams.CollectionAction.CREATESHARD.name(), 5000L);
    defaultOpDelays.put(CollectionParams.CollectionAction.DELETESHARD.name(), 5000L);
    defaultOpDelays.put(CollectionParams.CollectionAction.CREATE.name(), 500L);
    defaultOpDelays.put(CollectionParams.CollectionAction.DELETE.name(), 5000L);
  }

  // ============== SIMULATOR SETUP METHODS ====================

  public void simSetClusterState(ClusterState initialState) throws Exception {
    lock.lock();
    try {
      collProperties.clear();
      sliceProperties.clear();
      nodeReplicaMap.clear();
      liveNodes.clear();
      liveNodes.addAll(initialState.getLiveNodes());
      initialState.forEachCollection(dc -> {
        collProperties.computeIfAbsent(dc.getName(), name -> new ConcurrentHashMap<>()).putAll(dc.getProperties());
        opDelays.computeIfAbsent(dc.getName(), c -> new HashMap<>()).putAll(defaultOpDelays);
        dc.getSlices().forEach(s -> {
          sliceProperties.computeIfAbsent(dc.getName(), name -> new ConcurrentHashMap<>())
              .computeIfAbsent(s.getName(), name -> new HashMap<>()).putAll(s.getProperties());
          s.getReplicas().forEach(r -> {
            ReplicaInfo ri = new ReplicaInfo(r.getName(), r.getCoreName(), dc.getName(), s.getName(), r.getType(), r.getNodeName(), r.getProperties());
            if (liveNodes.contains(r.getNodeName())) {
              nodeReplicaMap.computeIfAbsent(r.getNodeName(), rn -> new ArrayList<>()).add(ri);
            }
          });
        });
      });
      saveClusterState();
    } finally {
      lock.unlock();
    }
  }

  public void simResetLeaderThrottle() {
    leaderThrottle.reset();
  }

  public String simGetRandomNode(Random random) {
    if (liveNodes.isEmpty()) {
      return null;
    }
    List<String> nodes = new ArrayList<>(liveNodes);
    return nodes.get(random.nextInt(nodes.size()));
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

  // utility to run leader election in a separate thread and with throttling
  // Note: leader election is a no-op if a shard leader already exists for each shard
  private class LeaderElection implements Callable<Boolean> {
    Collection<String> collections;
    boolean saveClusterState;

    LeaderElection(Collection<String> collections, boolean saveClusterState) {
      this.collections = collections;
      this.saveClusterState = saveClusterState;
    }

    @Override
    public Boolean call() {
      leaderThrottle.minimumWaitBetweenActions();
      leaderThrottle.markAttemptingAction();
      try {
        simRunLeaderElection(collections, saveClusterState);
      } catch (Exception e) {
        return false;
      }
      return true;
    }
  }

  // todo: maybe hook up DistribStateManager /live_nodes ?
  // todo: maybe hook up DistribStateManager /clusterstate.json watchers?
  public boolean simRemoveNode(String nodeId) throws Exception {
    lock.lock();
    try {
      Set<String> collections = new HashSet<>();
      // mark every replica on that node as down
      List<ReplicaInfo> replicas = nodeReplicaMap.get(nodeId);
      if (replicas != null) {
        replicas.forEach(r -> {
          r.getVariables().put(ZkStateReader.STATE_PROP, Replica.State.DOWN.toString());
          collections.add(r.getCollection());
        });
      }
      boolean res = liveNodes.remove(nodeId);
      if (!collections.isEmpty()) {
        cloudManager.submit(new LeaderElection(collections, true));
      }
      return res;
    } finally {
      lock.unlock();
    }
  }

  public void simAddReplica(ZkNodeProps message, NamedList results) throws Exception {
    AtomicLong policyVersionAfter = new AtomicLong(-1);
    ClusterState clusterState = getClusterState();
    DocCollection coll = clusterState.getCollection(message.getStr(ZkStateReader.COLLECTION_PROP));
    message = AddReplicaCmd.assignReplicaDetails(cloudManager, clusterState, message, policyVersionAfter);
    if (message.getStr(CoreAdminParams.CORE_NODE_NAME) == null) {
      message = message.plus(CoreAdminParams.CORE_NODE_NAME, Assign.assignCoreNodeName(stateManager, coll));
    }
    ReplicaInfo ri = new ReplicaInfo(
        message.getStr(CoreAdminParams.CORE_NODE_NAME),
        message.getStr(CoreAdminParams.NAME),
        message.getStr(ZkStateReader.COLLECTION_PROP),
        message.getStr(ZkStateReader.SHARD_ID_PROP),
        Replica.Type.valueOf(message.getStr(ZkStateReader.REPLICA_TYPE, Replica.Type.NRT.name()).toUpperCase(Locale.ROOT)),
        message.getStr(CoreAdminParams.NODE),
        message.getProperties()
    );
    simAddReplica(message.getStr(CoreAdminParams.NODE), ri, true);
  }

  // todo: maybe hook up DistribStateManager /clusterstate.json watchers?
  public void simAddReplica(String nodeId, ReplicaInfo replicaInfo, boolean runLeaderElection) throws Exception {
    // make sure coreNodeName is unique across cluster
    for (Map.Entry<String, List<ReplicaInfo>> e : nodeReplicaMap.entrySet()) {
      for (ReplicaInfo ri : e.getValue()) {
        if (ri.getCore().equals(replicaInfo.getCore())) {
          throw new Exception("Duplicate coreNodeName for existing=" + ri + " on node " + e.getKey() + " and new=" + replicaInfo);
        }
      }
    }
    if (!liveNodes.contains(nodeId)) {
      throw new Exception("Target node " + nodeId + " is not live: " + liveNodes);
    }
    // verify info
    if (replicaInfo.getCore() == null) {
      throw new Exception("Missing core: " + replicaInfo);
    }
//    if (replicaInfo.getShard() == null) {
//      throw new Exception("Missing shard: " + replicaInfo);
//    }
    replicaInfo.getVariables().remove(ZkStateReader.SHARD_ID_PROP);
    if (replicaInfo.getName() == null) {
      throw new Exception("Missing name: " + replicaInfo);
    }
    if (replicaInfo.getNode() == null) {
      throw new Exception("Missing node: " + replicaInfo);
    }
    if (!replicaInfo.getNode().equals(nodeId)) {
      throw new Exception("Wrong node (not " + nodeId + "): " + replicaInfo);
    }

    lock.lock();
    try {

      opDelay(replicaInfo.getCollection(), CollectionParams.CollectionAction.ADDREPLICA.name());

      List<ReplicaInfo> replicas = nodeReplicaMap.computeIfAbsent(nodeId, n -> new ArrayList<>());
      // mark replica as active
      replicaInfo.getVariables().put(ZkStateReader.STATE_PROP, Replica.State.ACTIVE.toString());
      // add property expected in tests
      replicaInfo.getVariables().put(Suggestion.coreidxsize, 123450000);
      // at this point nuke our cached DocCollection state
      collectionsStatesRef.set(null);

      replicas.add(replicaInfo);
      Map<String, Object> values = cloudManager.getSimNodeStateProvider().simGetAllNodeValues()
          .computeIfAbsent(nodeId, id -> new ConcurrentHashMap<>(SimCloudManager.createNodeValues(id)));
      // update the number of cores in node values
      Integer cores = (Integer)values.get("cores");
      if (cores == null) {
        cores = 0;
      }
      cloudManager.getSimNodeStateProvider().simSetNodeValue(nodeId, "cores", cores + 1);
      if (runLeaderElection) {
        cloudManager.submit(new LeaderElection(Collections.singleton(replicaInfo.getCollection()), true));
      }
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
        if (coreNodeName.equals(replicas.get(i).getName())) {
          ReplicaInfo ri = replicas.remove(i);

          opDelay(ri.getCollection(), CollectionParams.CollectionAction.DELETEREPLICA.name());

          // update the number of cores in node values, if node is live
          Integer cores = (Integer)cloudManager.getSimNodeStateProvider().simGetNodeValue(nodeId, "cores");
          if (liveNodes.contains(nodeId)) {
            if (cores == null || cores == 0) {
              throw new Exception("Unexpected value of 'cores' (" + cores + ") on node: " + nodeId);
            }
            cloudManager.getSimNodeStateProvider().simSetNodeValue(nodeId, "cores", cores - 1);
          }
          cloudManager.submit(new LeaderElection(Collections.singleton(ri.getCollection()), true));
          return;
        }
      }
      throw new Exception("Replica " + coreNodeName + " not found on node " + nodeId);
    } finally {
      lock.unlock();
    }
  }

  private ClusterState saveClusterState() throws IOException {
    collectionsStatesRef.set(null);
    ClusterState currentState = getClusterState();
    if (lastSavedState != null && lastSavedState.equals(currentState)) {
      return lastSavedState;
    }
    byte[] data = Utils.toJSON(currentState);
    try {
      VersionedData oldData = stateManager.getData(ZkStateReader.CLUSTER_STATE);
      int version = oldData != null ? oldData.getVersion() : -1;
      stateManager.setData(ZkStateReader.CLUSTER_STATE, data, version);
    } catch (Exception e) {
      throw new IOException(e);
    }
    lastSavedState = currentState;
    return currentState;
  }

  private void opDelay(String collection, String op) throws InterruptedException {
    Map<String, Long> delays = opDelays.get(collection);
    if (delays == null || delays.isEmpty() || !delays.containsKey(op)) {
      return;
    }
    cloudManager.getTimeSource().sleep(delays.get(op));
  }

  private synchronized void simRunLeaderElection(Collection<String> collections, boolean saveClusterState) throws Exception {
    ClusterState state = getClusterState();
    AtomicBoolean stateChanged = new AtomicBoolean(Boolean.FALSE);

    state.forEachCollection(dc -> {
      if (!collections.contains(dc.getName())) {
        return;
      }
      dc.getSlices().forEach(s -> {
        Replica leader = s.getLeader();
        if (leader == null || !liveNodes.contains(leader.getNodeName())) {
          LOG.trace("Running leader election for " + dc.getName() + " / " + s.getName());
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
              throw new IllegalStateException("-- could not find ReplicaInfo for replica " + r);
            }
            synchronized (ri) {
              if (ri.getVariables().remove(ZkStateReader.LEADER_PROP) != null) {
                stateChanged.set(true);
              }
              if (r.isActive(liveNodes)) {
                active.add(ri);
              } else { // if it's on a node that is not live mark it down
                if (!liveNodes.contains(r.getNodeName())) {
                  ri.getVariables().put(ZkStateReader.STATE_PROP, Replica.State.DOWN.toString());
                }
              }
            }
          });
          if (active.isEmpty()) {
            LOG.warn("-- can't find any active replicas for " + dc.getName() + " / " + s.getName());
            return;
          }
          // pick first active one
          ReplicaInfo ri = null;
          for (ReplicaInfo a : active) {
            if (!a.getType().equals(Replica.Type.PULL)) {
              ri = a;
              break;
            }
          }
          if (ri == null) {
            LOG.warn("-- can't find any suitable replica type for " + dc.getName() + " / " + s.getName());
            return;
          }
          synchronized (ri) {
            ri.getVariables().put(ZkStateReader.LEADER_PROP, "true");
          }
          stateChanged.set(true);
          LOG.info("-- elected new leader for " + dc.getName() + " / " + s.getName() + ": " + ri);
        } else {
          LOG.trace("-- already has leader for {} / {}", dc.getName(), s.getName());
        }
      });
    });
    if (saveClusterState || stateChanged.get()) {
      saveClusterState();
    }
  }

  public void simCreateCollection(ZkNodeProps props, NamedList results) throws Exception {
    if (props.getStr(CommonAdminParams.ASYNC) != null) {
      results.add(CoreAdminParams.REQUESTID, props.getStr(CommonAdminParams.ASYNC));
    }
    List<String> nodeList = new ArrayList<>();
    List<String> shardNames = new ArrayList<>();
    final String collectionName = props.getStr(NAME);
    ClusterState clusterState = getClusterState();
    ZkWriteCommand cmd = new ClusterStateMutator(cloudManager).createCollection(clusterState, props);
    if (cmd.noop) {
      LOG.warn("Collection {} already exists. exit", collectionName);
      results.add("success", "no-op");
      return;
    }
    opDelays.computeIfAbsent(collectionName, c -> new HashMap<>()).putAll(defaultOpDelays);

    opDelay(collectionName, CollectionParams.CollectionAction.CREATE.name());

    List<ReplicaPosition> replicaPositions = CreateCollectionCmd.buildReplicaPositions(cloudManager, getClusterState(), props,
        nodeList, shardNames);
    AtomicInteger replicaNum = new AtomicInteger(1);
    replicaPositions.forEach(pos -> {
      Map<String, Object> replicaProps = new HashMap<>();
      //replicaProps.put(ZkStateReader.SHARD_ID_PROP, pos.shard);
      replicaProps.put(ZkStateReader.NODE_NAME_PROP, pos.node);
      replicaProps.put(ZkStateReader.REPLICA_TYPE, pos.type.toString());
      String coreName = String.format(Locale.ROOT, "%s_%s_replica_%s%s", collectionName, pos.shard, pos.type.name().substring(0,1).toLowerCase(Locale.ROOT),
          replicaNum.getAndIncrement());
      try {
        replicaProps.put(ZkStateReader.CORE_NAME_PROP, coreName);
        ReplicaInfo ri = new ReplicaInfo("core_node" + Assign.incAndGetId(stateManager, collectionName, 0),
            coreName, collectionName, pos.shard, pos.type, pos.node, replicaProps);
        cloudManager.submit(() -> {simAddReplica(pos.node, ri, false); return true;});
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    collectionsStatesRef.set(null);
    // add collection props
    DocCollection coll = cmd.collection;
    collProperties.computeIfAbsent(collectionName, c -> new ConcurrentHashMap<>()).putAll(coll.getProperties());
    // add slice props
    coll.getSlices().forEach(s -> {
      Map<String, Object> sliceProps = sliceProperties.computeIfAbsent(coll.getName(), c -> new ConcurrentHashMap<>())
          .computeIfAbsent(s.getName(), slice -> new ConcurrentHashMap<>());
      s.getProperties().forEach((k, v) -> {
        if (k != null && v != null) {
          sliceProps.put(k, v);
        }
      });
    });
    cloudManager.submit(new LeaderElection(Collections.singleton(collectionName), true));
    results.add("success", "");
  }

  public void simDeleteCollection(String collection, String async, NamedList results) throws IOException {
    if (async != null) {
      results.add(CoreAdminParams.REQUESTID, async);
    }
    lock.lock();
    try {
      collProperties.remove(collection);
      sliceProperties.remove(collection);

      opDelay(collection, CollectionParams.CollectionAction.DELETE.name());

      opDelays.remove(collection);
      nodeReplicaMap.forEach((n, replicas) -> {
        for (Iterator<ReplicaInfo> it = replicas.iterator(); it.hasNext(); ) {
          ReplicaInfo ri = it.next();
          if (ri.getCollection().equals(collection)) {
            it.remove();
            // update the number of cores in node values
            Integer cores = (Integer) cloudManager.getSimNodeStateProvider().simGetNodeValue(n, "cores");
            if (cores != null) { // node is still up
              if (cores == 0) {
                throw new RuntimeException("Unexpected value of 'cores' (" + cores + ") on node: " + n);
              }
              cloudManager.getSimNodeStateProvider().simSetNodeValue(n, "cores", cores - 1);
            }
          }
        }
      });
      saveClusterState();
      results.add("success", "");
    } catch (Exception e) {
      LOG.warn("Exception", e);
    } finally {
      lock.unlock();
    }
  }

  public void simDeleteAllCollections() throws Exception {
    lock.lock();
    try {
      nodeReplicaMap.clear();
      collProperties.clear();
      sliceProperties.clear();
      cloudManager.getSimNodeStateProvider().simGetAllNodeValues().forEach((n, values) -> {
        values.put("cores", 0);
      });
      saveClusterState();
    } finally {
      lock.unlock();
    }
  }

  public void simMoveReplica(ZkNodeProps message, NamedList results) throws Exception {
    if (message.getStr(CommonAdminParams.ASYNC) != null) {
      results.add(CoreAdminParams.REQUESTID, message.getStr(CommonAdminParams.ASYNC));
    }
    String collection = message.getStr(COLLECTION_PROP);
    String targetNode = message.getStr("targetNode");
    ClusterState clusterState = getClusterState();
    DocCollection coll = clusterState.getCollection(collection);
    if (coll == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collection: " + collection + " does not exist");
    }
    String replicaName = message.getStr(REPLICA_PROP);
    Replica replica = coll.getReplica(replicaName);
    if (replica == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Collection: " + collection + " replica: " + replicaName + " does not exist");
    }
    Slice slice = null;
    for (Slice s : coll.getSlices()) {
      if (s.getReplicas().contains(replica)) {
        slice = s;
      }
    }
    if (slice == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Replica has no 'slice' property! : " + replica);
    }

    opDelay(collection, CollectionParams.CollectionAction.MOVEREPLICA.name());

    // TODO: for now simulate moveNormalReplica sequence, where we first add new replica and then delete the old one

    String newSolrCoreName = Assign.buildSolrCoreName(stateManager, coll, slice.getName(), replica.getType());
    String coreNodeName = Assign.assignCoreNodeName(stateManager, coll);
    ReplicaInfo newReplica = new ReplicaInfo(coreNodeName, newSolrCoreName, collection, slice.getName(), replica.getType(), targetNode, null);
    LOG.debug("-- new replica: " + newReplica);
    // xxx should run leader election here already?
    simAddReplica(targetNode, newReplica, false);
    // this will trigger leader election
    simRemoveReplica(replica.getNodeName(), replica.getName());
    results.add("success", "");
  }

  public void simCreateShard(ZkNodeProps message, NamedList results) throws Exception {
    if (message.getStr(CommonAdminParams.ASYNC) != null) {
      results.add(CoreAdminParams.REQUESTID, message.getStr(CommonAdminParams.ASYNC));
    }
    String collectionName = message.getStr(COLLECTION_PROP);
    String sliceName = message.getStr(SHARD_ID_PROP);
    ClusterState clusterState = getClusterState();
    lock.lock();
    try {
      ZkWriteCommand cmd = new CollectionMutator(cloudManager).createShard(clusterState, message);
      if (cmd.noop) {
        results.add("success", "no-op");
        return;
      }

      opDelay(collectionName, CollectionParams.CollectionAction.CREATESHARD.name());

      // copy shard properties -- our equivalent of creating an empty shard in cluster state
      DocCollection collection = cmd.collection;
      Slice slice = collection.getSlice(sliceName);
      Map<String, Object> props = sliceProperties.computeIfAbsent(collection.getName(), c -> new ConcurrentHashMap<>())
          .computeIfAbsent(sliceName, s -> new ConcurrentHashMap<>());
      props.clear();
      slice.getProperties().entrySet().stream()
          .filter(e -> !e.getKey().equals("range"))
          .filter(e -> !e.getKey().equals("replicas"))
          .forEach(e -> props.put(e.getKey(), e.getValue()));
      // 2. create new replicas
      List<ReplicaPosition> positions = CreateShardCmd.buildReplicaPositions(cloudManager, clusterState, collectionName, message);
      AtomicInteger replicaNum = new AtomicInteger(1);
      positions.forEach(pos -> {
        Map<String, Object> replicaProps = new HashMap<>();
        replicaProps.put(ZkStateReader.SHARD_ID_PROP, pos.shard);
        replicaProps.put(ZkStateReader.NODE_NAME_PROP, pos.node);
        replicaProps.put(ZkStateReader.REPLICA_TYPE, pos.type.toString());
        replicaProps.put(ZkStateReader.BASE_URL_PROP, Utils.getBaseUrlForNodeName(pos.node, "http"));
        String coreName = String.format(Locale.ROOT, "%s_%s_replica_%s%s", collectionName, pos.shard, pos.type.name().substring(0,1).toLowerCase(Locale.ROOT),
            replicaNum.getAndIncrement());
        try {
          replicaProps.put(ZkStateReader.CORE_NAME_PROP, coreName);
          ReplicaInfo ri = new ReplicaInfo("core_node" + Assign.incAndGetId(stateManager, collectionName, 0),
              coreName, collectionName, pos.shard, pos.type, pos.node, replicaProps);
          simAddReplica(pos.node, ri, false);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
      Map<String, Object> colProps = collProperties.computeIfAbsent(collectionName, c -> new ConcurrentHashMap<>());

      cloudManager.submit(new LeaderElection(Collections.singleton(collectionName), true));
      results.add("success", "");
    } finally {
      lock.unlock();
    }
  }

  public void simSplitShard(ZkNodeProps message, NamedList results) throws Exception {
    String collectionName = message.getStr(COLLECTION_PROP);
    AtomicReference<String> sliceName = new AtomicReference<>();
    sliceName.set(message.getStr(SHARD_ID_PROP));
    String splitKey = message.getStr("split.key");
    ClusterState clusterState = getClusterState();
    DocCollection collection = clusterState.getCollection(collectionName);
    Slice parentSlice = SplitShardCmd.getParentSlice(clusterState, collectionName, sliceName, splitKey);
    List<DocRouter.Range> subRanges = new ArrayList<>();
    List<String> subSlices = new ArrayList<>();
    List<String> subShardNames = new ArrayList<>();

    opDelay(collectionName, CollectionParams.CollectionAction.SPLITSHARD.name());

    SplitShardCmd.fillRanges(cloudManager, message, collection, parentSlice, subRanges, subSlices, subShardNames);
    // mark the old slice as inactive
    sliceProperties.computeIfAbsent(collectionName, c -> new ConcurrentHashMap<>())
        .computeIfAbsent(sliceName.get(), s -> new ConcurrentHashMap<>())
        .put(ZkStateReader.SHARD_STATE_PROP, Slice.State.INACTIVE.toString());
    // add slice props
    for (int i = 0; i < subRanges.size(); i++) {
      String subSlice = subSlices.get(i);
      DocRouter.Range range = subRanges.get(i);
      Map<String, Object> sliceProps = sliceProperties.computeIfAbsent(collectionName, c -> new ConcurrentHashMap<>())
          .computeIfAbsent(subSlice, ss -> new ConcurrentHashMap<>());
      sliceProps.put(Slice.RANGE, range);
      sliceProps.put(Slice.PARENT, sliceName.get());
      sliceProps.put(ZkStateReader.SHARD_STATE_PROP, Slice.State.ACTIVE.toString());
    }
    // add replicas for new subShards
    int repFactor = parentSlice.getReplicas().size();
    List<ReplicaPosition> replicaPositions = Assign.identifyNodes(cloudManager,
        clusterState,
        new ArrayList<>(clusterState.getLiveNodes()),
        collectionName,
        new ZkNodeProps(collection.getProperties()),
        // reproduce the bug
        subSlices, repFactor, 0, 0);

    for (ReplicaPosition replicaPosition : replicaPositions) {
      String subSliceName = replicaPosition.shard;
      String subShardNodeName = replicaPosition.node;
      String solrCoreName = collectionName + "_" + subSliceName + "_replica" + (replicaPosition.index);
      Map<String, Object> replicaProps = new HashMap<>();
      replicaProps.put(ZkStateReader.SHARD_ID_PROP, replicaPosition.shard);
      replicaProps.put(ZkStateReader.NODE_NAME_PROP, replicaPosition.node);
      replicaProps.put(ZkStateReader.REPLICA_TYPE, replicaPosition.type.toString());
      replicaProps.put(ZkStateReader.BASE_URL_PROP, Utils.getBaseUrlForNodeName(subShardNodeName, "http"));

      ReplicaInfo ri = new ReplicaInfo("core_node" + Assign.incAndGetId(stateManager, collectionName, 0),
          solrCoreName, collectionName, replicaPosition.shard, replicaPosition.type, subShardNodeName, replicaProps);
      simAddReplica(replicaPosition.node, ri, false);
    }
    cloudManager.submit(new LeaderElection(Collections.singleton(collectionName), true));
    results.add("success", "");

  }

  public void simDeleteShard(ZkNodeProps message, NamedList results) throws Exception {
    String collectionName = message.getStr(COLLECTION_PROP);
    String sliceName = message.getStr(SHARD_ID_PROP);
    ClusterState clusterState = getClusterState();
    DocCollection collection = clusterState.getCollection(collectionName);
    if (collection == null) {
      throw new Exception("Collection " + collectionName + " doesn't exist");
    }
    Slice slice = collection.getSlice(sliceName);
    if (slice == null) {
      throw new Exception(" Collection " + collectionName + " slice " + sliceName + " doesn't exist.");
    }

    opDelay(collectionName, CollectionParams.CollectionAction.DELETESHARD.name());

    lock.lock();
    try {
      sliceProperties.computeIfAbsent(collectionName, coll -> new ConcurrentHashMap<>()).remove(sliceName);
      nodeReplicaMap.forEach((n, replicas) -> {
        Iterator<ReplicaInfo> it = replicas.iterator();
        while (it.hasNext()) {
          ReplicaInfo ri = it.next();
          if (ri.getCollection().equals(collectionName) && ri.getShard().equals(sliceName)) {
            it.remove();
          }
        }
      });
      results.add("success", "");
    } catch (Exception e) {
      results.add("failure", e.toString());
    } finally {
      lock.unlock();
    }
  }

  public synchronized Map<String, Object> saveClusterProperties() throws Exception {
    if (lastSavedProperties != null && lastSavedProperties.equals(clusterProperties)) {
      return lastSavedProperties;
    }
    byte[] data = Utils.toJSON(clusterProperties);
    VersionedData oldData = stateManager.getData(ZkStateReader.CLUSTER_PROPS);
    int version = oldData != null ? oldData.getVersion() : -1;
    stateManager.setData(ZkStateReader.CLUSTER_PROPS, data, version);
    lastSavedProperties = (Map)Utils.fromJSON(data);
    return lastSavedProperties;
  }

  public void simSetClusterProperties(Map<String, Object> properties) throws Exception {
    lock.lock();
    try {
      clusterProperties.clear();
      if (properties != null) {
        this.clusterProperties.putAll(properties);
      }
      saveClusterProperties();
    } finally {
      lock.unlock();
    }
  }

  public void simSetClusterProperty(String key, Object value) throws Exception {
    lock.lock();
    try {
      if (value != null) {
        clusterProperties.put(key, value);
      } else {
        clusterProperties.remove(key);
      }
      saveClusterProperties();
    } finally {
      lock.unlock();
    }
  }

  public void simSetCollectionProperties(String coll, Map<String, Object> properties) throws Exception {
    if (properties == null) {
      collProperties.remove(coll);
      saveClusterState();
    } else {
      lock.lock();
      try {
        Map<String, Object> props = collProperties.computeIfAbsent(coll, c -> new HashMap<>());
        props.clear();
        props.putAll(properties);
        saveClusterState();
      } finally {
        lock.unlock();
      }
    }
  }

  public void simSetCollectionProperty(String coll, String key, String value) throws Exception {
    Map<String, Object> props = collProperties.computeIfAbsent(coll, c -> new HashMap<>());
    if (value == null) {
      props.remove(key);
    } else {
      props.put(key, value);
    }
    saveClusterState();
  }

  public void simSetSliceProperties(String coll, String slice, Map<String, Object> properties) throws Exception {
    Map<String, Object> sliceProps = sliceProperties.computeIfAbsent(coll, c -> new HashMap<>()).computeIfAbsent(slice, s -> new HashMap<>());
    lock.lock();
    try {
      sliceProps.clear();
      if (properties != null) {
        sliceProps.putAll(properties);
      }
      saveClusterState();
    } finally {
      lock.unlock();
    }
  }

  public void simSetCollectionValue(String collection, String key, Object value, boolean divide) throws Exception {
    List<ReplicaInfo> infos = new ArrayList<>();
    nodeReplicaMap.forEach((n, replicas) -> {
      replicas.forEach(r -> {
        if (r.getCollection().equals(collection)) {
          infos.add(r);
        }
      });
    });
    if (infos.isEmpty()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collection " + collection + " doesn't exist.");
    }
    if (divide && value != null && (value instanceof Number)) {
      value = ((Number)value).doubleValue() / infos.size();
    }
    for (ReplicaInfo r : infos) {
      if (value == null) {
        r.getVariables().remove(key);
      } else {
        r.getVariables().put(key, value);
      }
    }
  }

  public void simSetCollectionValue(String collection, String key, Object value) throws Exception {
    simSetCollectionValue(collection, key, value, false);
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
    Map<String, DocCollection> collectionStates = collectionsStatesRef.get();
    if (collectionStates != null) {
      return collectionStates;
    }
    lock.lock();
    try {
      Map<String, Map<String, Map<String, Replica>>> collMap = new HashMap<>();
      nodeReplicaMap.forEach((n, replicas) -> {
        replicas.forEach(ri -> {
          Map<String, Object> props;
          synchronized (ri) {
            props = new HashMap<>(ri.getVariables());
          }
          props.put(ZkStateReader.NODE_NAME_PROP, n);
          //props.put(ZkStateReader.SHARD_ID_PROP, ri.getShard());
          props.put(ZkStateReader.CORE_NAME_PROP, ri.getCore());
          props.put(ZkStateReader.REPLICA_TYPE, ri.getType().toString());
          props.put(ZkStateReader.STATE_PROP, ri.getState().toString());
          Replica r = new Replica(ri.getName(), props);
          collMap.computeIfAbsent(ri.getCollection(), c -> new HashMap<>())
              .computeIfAbsent(ri.getShard(), s -> new HashMap<>())
              .put(ri.getName(), r);
        });
      });

      // add empty slices
      sliceProperties.forEach((c, perSliceProps) -> {
        perSliceProps.forEach((slice, props) -> {
          collMap.computeIfAbsent(c, co -> new ConcurrentHashMap<>()).computeIfAbsent(slice, s -> new ConcurrentHashMap<>());
        });
      });
      // add empty collections
      collProperties.keySet().forEach(c -> {
        collMap.computeIfAbsent(c, co -> new ConcurrentHashMap<>());
      });

      Map<String, DocCollection> res = new HashMap<>();
      collMap.forEach((coll, shards) -> {
        Map<String, Slice> slices = new HashMap<>();
        shards.forEach((s, replicas) -> {
          Map<String, Object> sliceProps = sliceProperties.computeIfAbsent(coll, c -> new ConcurrentHashMap<>()).computeIfAbsent(s, sl -> new ConcurrentHashMap<>());
          Slice slice = new Slice(s, replicas, sliceProps);
          slices.put(s, slice);
        });
        Map<String, Object> collProps = collProperties.computeIfAbsent(coll, c -> new ConcurrentHashMap<>());
        DocCollection dc = new DocCollection(coll, slices, collProps, DocRouter.DEFAULT, 0, ZkStateReader.CLUSTER_STATE);
        res.put(coll, dc);
      });
      collectionsStatesRef.set(res);
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

  }
}
