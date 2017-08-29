package org.apache.solr.cloud.autoscaling;

import java.io.IOException;

import org.apache.solr.client.solrj.cloud.DistributedQueue;
import org.apache.solr.client.solrj.cloud.autoscaling.SolrCloudDataProvider;
import org.apache.solr.cloud.ZkDistributedQueue;
import org.apache.solr.common.cloud.SolrZkClient;

/**
 *
 */
public class ZkDistributedQueueFactory implements SolrCloudDataProvider.DistributedQueueFactory {
  private final SolrZkClient zkClient;

  public ZkDistributedQueueFactory(SolrZkClient zkClient) {
    this.zkClient = zkClient;
  }
  @Override
  public DistributedQueue makeQueue(String path) throws IOException {
    return new ZkDistributedQueue(zkClient, path);
  }

  @Override
  public void removeQueue(String path) throws IOException {

  }
}
