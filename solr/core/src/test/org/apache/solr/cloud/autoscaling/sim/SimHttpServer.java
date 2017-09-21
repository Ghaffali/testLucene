package org.apache.solr.cloud.autoscaling.sim;

import java.io.IOException;
import java.util.Map;

import org.apache.solr.client.solrj.SolrRequest;

/**
 *
 */
public class SimHttpServer {

  public byte[] httpRequest(String url, SolrRequest.METHOD method, Map<String, String> headers, String payload, int timeout, boolean followRedirects) throws IOException {
    return new byte[0];
  }
}
