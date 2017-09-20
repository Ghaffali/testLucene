package org.apache.solr.cloud.autoscaling.sim;

import java.io.IOException;
import java.util.Map;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.cloud.autoscaling.ClusterDataProvider;
import org.apache.solr.client.solrj.cloud.autoscaling.DistribStateManager;
import org.apache.solr.client.solrj.cloud.autoscaling.SolrCloudDataProvider;

/**
 *
 */
public class SimCloudDataProvider implements SolrCloudDataProvider {

  private final DistribStateManager stateManager;
  private final ClusterDataProvider dataProvider;
  private final DistributedQueueFactory queueFactory;

  public SimCloudDataProvider(DistribStateManager stateManager, ClusterDataProvider dataProvider, DistributedQueueFactory queueFactory) {
    this.stateManager = stateManager;
    this.dataProvider = dataProvider;
    this.queueFactory = queueFactory;
  }

  @Override
  public ClusterDataProvider getClusterDataProvider() {
    return dataProvider;
  }

  @Override
  public DistribStateManager getDistribStateManager() {
    return stateManager;
  }

  @Override
  public DistributedQueueFactory getDistributedQueueFactory() {
    return queueFactory;
  }

  @Override
  public SolrResponse request(SolrRequest req) throws IOException {
    return null;
  }

  @Override
  public byte[] httpRequest(String url, SolrRequest.METHOD method, Map<String, String> headers, String payload, int timeout, boolean followRedirects) throws IOException {
    return new byte[0];
  }
}
