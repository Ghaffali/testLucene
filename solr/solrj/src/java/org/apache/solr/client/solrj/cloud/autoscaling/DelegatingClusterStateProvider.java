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
package org.apache.solr.client.solrj.cloud.autoscaling;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.common.cloud.ClusterState;

/**
 *
 */
public class DelegatingClusterStateProvider implements ClusterStateProvider {
  protected ClusterStateProvider delegate;

  public DelegatingClusterStateProvider(ClusterStateProvider delegate) {
    this.delegate = delegate;
  }

  @Override
  public ClusterState.CollectionRef getState(String collection) {
    return delegate.getState(collection);
  }

  @Override
  public Set<String> getLiveNodes() {
    return delegate.getLiveNodes();
  }

  @Override
  public String getAlias(String alias) {
    return delegate.getAlias(alias);
  }

  @Override
  public String getCollectionName(String name) {
    return delegate.getCollectionName(name);
  }

  @Override
  public ClusterState getClusterState() throws IOException {
    return delegate.getClusterState();
  }

  @Override
  public Map<String, Object> getClusterProperties() {
    return delegate.getClusterProperties();
  }

  @Override
  public String getPolicyNameByCollection(String coll) {
    return delegate.getPolicyNameByCollection(coll);
  }

  @Override
  public void connect() {
    delegate.connect();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
