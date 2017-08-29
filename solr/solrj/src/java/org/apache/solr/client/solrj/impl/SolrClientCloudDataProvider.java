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

package org.apache.solr.client.solrj.impl;


import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.autoscaling.ClusterDataProvider;
import org.apache.solr.client.solrj.cloud.autoscaling.SolrCloudDataProvider;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that implements {@link SolrCloudDataProvider} using a SolrClient
 */
public class SolrClientCloudDataProvider implements SolrCloudDataProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CloudSolrClient solrClient;
  private final SolrClientClusterDataProvider clusterDataProvider;
  private final DistributedQueueFactory queueFactory;
  private final ZkStateReader zkStateReader;
  private final SolrZkClient zkClient;

  public SolrClientCloudDataProvider(DistributedQueueFactory queueFactory, CloudSolrClient solrClient) {
    this.queueFactory = queueFactory;
    this.solrClient = solrClient;
    this.clusterDataProvider = new SolrClientClusterDataProvider(solrClient);
    this.zkStateReader = solrClient.getZkStateReader();
    this.zkClient = zkStateReader.getZkClient();
  }

  @Override
  public ClusterDataProvider getClusterDataProvider() {
    return clusterDataProvider;
  }

  @Override
  public boolean hasData(String path) throws IOException {
    try {
      return zkClient.exists(path, true);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public List<String> listData(String path) throws NoSuchElementException, IOException {
    try {
      return zkClient.getChildren(path, null, true);
    } catch (KeeperException.NoNodeException e) {
      throw new NoSuchElementException(path);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public VersionedData getData(String path, Watcher watcher) throws NoSuchElementException, IOException {
    Stat stat = new Stat();
    try {
      byte[] bytes = zkClient.getData(path, watcher, stat, true);
      return new VersionedData(stat.getVersion(), bytes);
    } catch (KeeperException.NoNodeException e) {
      throw new NoSuchElementException(path);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void makePath(String path) throws IOException {
    try {
      zkClient.makePath(path, true);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void createData(String path, byte[] data, CreateMode mode) throws IOException {
    try {
      zkClient.create(path, data, mode, true);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void removeData(String path, int version) throws NoSuchElementException, IOException {
    try {
      zkClient.delete(path, version, true);
    } catch (KeeperException.NoNodeException e) {
      throw new NoSuchElementException(path);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setData(String path, byte[] data, int version) throws NoSuchElementException, IOException {
    try {
      zkClient.setData(path, data, version, true);
    } catch (KeeperException.NoNodeException e) {
      throw new NoSuchElementException(path);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public SolrResponse request(SolrRequest req) throws IOException {
    try {
      return req.process(solrClient);
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
  }

  private static final byte[] EMPTY = new byte[0];

  @Override
  public byte[] httpRequest(String url, SolrRequest.METHOD method, Map<String, String> headers, String payload, int timeout, boolean followRedirects) throws IOException {
    HttpClient client = solrClient.getHttpClient();
    final HttpRequestBase req;
    HttpEntity entity = null;
    if (payload != null) {
      entity = new StringEntity(payload, "UTF-8");
    }
    switch (method) {
      case GET:
        req = new HttpGet(url);
        break;
      case POST:
        req = new HttpPost(url);
        if (entity != null) {
          ((HttpPost)req).setEntity(entity);
        }
        break;
      case PUT:
        req = new HttpPut(url);
        if (entity != null) {
          ((HttpPut)req).setEntity(entity);
        }
        break;
      case DELETE:
        req = new HttpDelete(url);
        break;
      default:
        throw new IOException("Unsupported method " + method);
    }
    if (headers != null) {
      headers.forEach((k, v) -> req.addHeader(k, v));
    }
    RequestConfig.Builder requestConfigBuilder = HttpClientUtil.createDefaultRequestConfigBuilder();
    if (timeout > 0) {
      requestConfigBuilder.setSocketTimeout(timeout);
      requestConfigBuilder.setConnectTimeout(timeout);
    }
    requestConfigBuilder.setRedirectsEnabled(followRedirects);
    req.setConfig(requestConfigBuilder.build());
    HttpClientContext httpClientRequestContext = HttpClientUtil.createNewHttpClientRequestContext();
    HttpResponse rsp = client.execute(req, httpClientRequestContext);
    int statusCode = rsp.getStatusLine().getStatusCode();
    if (statusCode != 200) {
      throw new IOException("Error sending request to " + url + ", HTTP response: " + rsp.toString());
    }
    HttpEntity responseEntity = rsp.getEntity();
    if (responseEntity != null && responseEntity.getContent() != null) {
      return EntityUtils.toByteArray(responseEntity);
    } else {
      return EMPTY;
    }
  }

  @Override
  public List<OpResult> multi(Iterable<Op> ops) throws IOException {
    try {
      return zkClient.multi(ops, true);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public DistributedQueueFactory getDistributedQueueFactory() {
    return queueFactory;
  }

}
