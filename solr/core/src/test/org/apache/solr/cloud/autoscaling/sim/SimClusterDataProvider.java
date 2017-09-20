package org.apache.solr.cloud.autoscaling.sim;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.ClusterDataProvider;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.zookeeper.Watcher;

/**
 *
 */
public class SimClusterDataProvider implements ClusterDataProvider {
  @Override
  public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
    return null;
  }

  @Override
  public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
    return null;
  }

  @Override
  public Collection<String> getLiveNodes() {
    return null;
  }

  @Override
  public ClusterState getClusterState() throws IOException {
    return null;
  }

  @Override
  public Map<String, Object> getClusterProperties() {
    return null;
  }

  @Override
  public AutoScalingConfig getAutoScalingConfig(Watcher watcher) throws InterruptedException, IOException {
    return null;
  }

  @Override
  public String getPolicyNameByCollection(String coll) {
    return null;
  }
}
