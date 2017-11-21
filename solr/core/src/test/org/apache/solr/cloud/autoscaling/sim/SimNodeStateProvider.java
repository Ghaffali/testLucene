package org.apache.solr.cloud.autoscaling.sim;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.cloud.autoscaling.BadVersionException;
import org.apache.solr.client.solrj.cloud.autoscaling.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SimNodeStateProvider implements NodeStateProvider {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<String, Map<String, Object>> nodeValues = new ConcurrentHashMap<>();
  private final SimClusterStateProvider clusterStateProvider;
  private final SimDistribStateManager stateManager;
  private final Set<String> liveNodes;

  public SimNodeStateProvider(Set<String> liveNodes, SimDistribStateManager stateManager,
                              SimClusterStateProvider clusterStateProvider,
                              Map<String, Map<String, Object>> nodeValues) {
    this.liveNodes = liveNodes;
    this.stateManager = stateManager;
    this.clusterStateProvider = clusterStateProvider;
    if (nodeValues != null) {
      this.nodeValues.putAll(nodeValues);
    }
  }

  // -------- simulator setup methods ------------
  public Object simGetNodeValue(String node, String key) {
    Map<String, Object> values = nodeValues.get(node);
    if (values == null) {
      return null;
    }
    return values.get(key);
  }

  public void simSetNodeValues(String node, Map<String, Object> values) {
    nodeValues.put(node, new ConcurrentHashMap<>(values));
    if (values.containsKey("nodeRole")) {
      saveRoles();
    }
  }

  public void simSetNodeValue(String node, String key, Object value) {
    nodeValues.computeIfAbsent(node, n -> new ConcurrentHashMap<>()).put(key, value);
    if (key.equals("nodeRole")) {
      saveRoles();
    }
  }

  public void simAddNodeValue(String node, String key, Object value) {
    Map<String, Object> values = nodeValues.computeIfAbsent(node, n -> new ConcurrentHashMap<>());
    Object existing = values.get(key);
    if (existing == null) {
      values.put(key, value);
    } else if (existing instanceof Set) {
      ((Set)existing).add(value);
    } else {
      Set<Object> vals = new HashSet<>();
      vals.add(existing);
      vals.add(value);
      values.put(key, vals);
    }
    if (key.equals("nodeRole")) {
      saveRoles();
    }
  }

  public void simRemoveNodeValues(String node) {
    Map<String, Object> values = nodeValues.remove(node);
    if (values != null && values.containsKey("nodeRole")) {
      saveRoles();
    }
  }

  public Map<String, Map<String, Object>> simGetAllNodeValues() {
    return nodeValues;
  }

  private synchronized void saveRoles() {
    final Map<String, Set<String>> roles = new HashMap<>();
    nodeValues.forEach((n, values) -> {
      String nodeRole = (String)values.get("nodeRole");
      if (nodeRole != null) {
        roles.computeIfAbsent(nodeRole, role -> new HashSet<>()).add(n);
      }
    });
    try {
      stateManager.setData(ZkStateReader.ROLES, Utils.toJSON(roles), -1);
    } catch (Exception e) {
      throw new RuntimeException("Unexpected exception saving roles " + roles, e);
    }
  }

  // ---------- interface methods -------------

  @Override
  public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
    LOG.debug("-- requested values for " + node + ": " + tags);
    if (!liveNodes.contains(node)) {
      nodeValues.remove(node);
      return Collections.emptyMap();
    }
    Map<String, Object> values = nodeValues.get(node);
    if (values == null) {
      return Collections.emptyMap();
    }
    return values.entrySet().stream().filter(e -> tags.contains(e.getKey())).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
  }

  @Override
  public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
    List<ReplicaInfo> replicas = clusterStateProvider.simGetReplicaInfos(node);
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
  public void close() throws IOException {

  }
}
