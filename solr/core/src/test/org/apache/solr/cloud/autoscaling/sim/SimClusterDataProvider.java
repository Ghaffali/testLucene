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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.ClusterDataProvider;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 *
 */
public class SimClusterDataProvider implements ClusterDataProvider {

  private final Map<String, List<ReplicaInfo>> nodes = new ConcurrentHashMap<>();
  private final Map<String, Map<String, Object>> nodeValues = new ConcurrentHashMap<>();

  private AutoScalingConfig autoScalingConfig = new AutoScalingConfig(Collections.emptyMap());
  private List<Watcher> autoscalingWatchers = new ArrayList<>();
  private Map<String, Object> clusterProperties = new ConcurrentHashMap<>();
  private Map<String, Map<String, Object>> collProperties = new ConcurrentHashMap<>();
  private Map<String, Map<String, Map<String, Object>>> sliceProperties = new ConcurrentHashMap<>();

  // ============== SIMULATOR SETUP METHODS ====================

  public void simAddNodes(Collection<String> nodeIds) {
    for (String node : nodeIds) {
      simAddNode(node);
    }
  }
  // todo: maybe hook up DistribStateManager /live_nodes ?
  // todo: maybe hook up DistribStateManager /clusterstate.json watchers?
  public void simAddNode(String nodeId) {
    nodes.putIfAbsent(nodeId, new ArrayList<>());
  }

  // todo: maybe hook up DistribStateManager /live_nodes ?
  // todo: maybe hook up DistribStateManager /clusterstate.json watchers?
  public void simRemoveNode(String nodeId) {
    nodes.remove(nodeId);
    nodeValues.remove(nodeId);
  }

  // todo: maybe hook up DistribStateManager /clusterstate.json watchers?
  public void simAddReplica(String nodeId, ReplicaInfo replicaInfo) {
    List<ReplicaInfo> replicas = nodes.computeIfAbsent(nodeId, n -> new ArrayList<>());
    // make sure coreNodeName is unique
    for (ReplicaInfo ri : replicas) {
      if (ri.getCore().equals(replicaInfo.getCore())) {
        throw new RuntimeException("Duplicate coreNodeName for existing=" + ri + " and new=" + replicaInfo);
      }
    }
    replicas.add(replicaInfo);
  }

  // todo: maybe hook up DistribStateManager /clusterstate.json watchers?
  public void simRemoveReplica(String nodeId, String coreNodeName) throws IOException {
    List<ReplicaInfo> replicas = nodes.computeIfAbsent(nodeId, n -> new ArrayList<>());
    for (int i = 0; i < replicas.size(); i++) {
      if (coreNodeName.equals(replicas.get(i).getCore())) {
        replicas.remove(i);
        return;
      }
    }
    throw new IOException("Replica " + coreNodeName + " not found on node " + nodeId);
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
