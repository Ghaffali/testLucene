package org.apache.solr.client.solrj.cloud.autoscaling;

import java.io.IOException;
import java.net.ConnectException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.cloud.ClusterState;
import org.apache.zookeeper.Watcher;

/**
 *
 */
public class DelegatingClusterDataProvider implements ClusterDataProvider {
  protected ClusterDataProvider delegate;

  public DelegatingClusterDataProvider(ClusterDataProvider delegate) {
    this.delegate = delegate;
  }

  @Override
  public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
    return delegate.getNodeValues(node, tags);
  }

  @Override
  public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
    return delegate.getReplicaInfo(node, keys);
  }

  @Override
  public Collection<String> getLiveNodes() {
    return delegate.getLiveNodes();
  }

  @Override
  public Map<String, Object> getClusterProperties() {
    return delegate.getClusterProperties();
  }

  @Override
  public ClusterState getClusterState() throws IOException {
    return delegate.getClusterState();
  }

  @Override
  public AutoScalingConfig getAutoScalingConfig(Watcher watcher) throws ConnectException, InterruptedException, IOException {
    return delegate.getAutoScalingConfig(watcher);
  }

  @Override
  public String getPolicyNameByCollection(String coll) {
    return delegate.getPolicyNameByCollection(coll);
  }

}
