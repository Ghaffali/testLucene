package org.apache.solr.cloud.autoscaling.sim;

import org.apache.solr.client.solrj.cloud.DistributedQueue;
import org.apache.solr.client.solrj.cloud.autoscaling.DistribStateManager;

/**
 *
 */
public class TestGenericDistributedQueue extends TestSimDistributedQueue {
  DistribStateManager stateManager = new SimDistribStateManager();

  @Override
  protected DistributedQueue makeDistributedQueue(String dqZNode) throws Exception {
    return new GenericDistributedQueue(stateManager, dqZNode);
  }
}
