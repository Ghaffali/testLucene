package org.apache.solr.cloud.autoscaling.sim;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.cloud.autoscaling.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SimNodeStateProvider implements NodeStateProvider {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<String, Map<String, Object>> nodeValues = new ConcurrentHashMap<>();
  private final SimClusterStateProvider stateProvider;

  public SimNodeStateProvider(SimClusterStateProvider stateProvider, Map<String, Map<String, Object>> nodeValues) {
    this.stateProvider = stateProvider;
    if (nodeValues != null) {
      this.nodeValues.putAll(nodeValues);
    }
  }

  // -------- simulator setup methods ------------
  public void simSetNodeValues(String node, Map<String, Object> values) {
    nodeValues.put(node, new ConcurrentHashMap<>(values));
  }

  public void simSetNodeValue(String node, String key, Object value) {
    nodeValues.computeIfAbsent(node, n -> new ConcurrentHashMap<>()).put(key, value);
  }

  // ---------- interface methods -------------

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
    List<ReplicaInfo> replicas = stateProvider.simGetReplicaInfos(node);
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

}
