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
package org.apache.solr.cloud.autoscaling;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.SolrClientDataProvider;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.TimeSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Trigger for the {@link org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType#SEARCHRATE} event.
 */
public class SearchRateTrigger extends TriggerBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CoreContainer container;
  private final TimeSource timeSource;
  private final CloudSolrClient cloudSolrClient;
  private final SolrClientDataProvider dataProvider;

  private Set<String> lastLiveNodes;

  public SearchRateTrigger(String name, Map<String, Object> properties, CoreContainer container) {
    super(name, properties, container.getResourceLoader(), container.getZkController().getZkClient());
    this.container = container;
    this.timeSource = TimeSource.CURRENT_TIME;
    this.lastLiveNodes = new HashSet<>(container.getZkController().getZkStateReader().getClusterState().getLiveNodes());
    this.cloudSolrClient = new CloudSolrClient.Builder()
        .withClusterStateProvider(new ZkClientClusterStateProvider(container.getZkController().getZkStateReader()))
        .build();
    this.dataProvider = new SolrClientDataProvider(cloudSolrClient);
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (dataProvider != null) {
      IOUtils.closeQuietly(dataProvider);
    }
    if (cloudSolrClient != null) {
      IOUtils.closeQuietly(cloudSolrClient);
    }
  }

  @Override
  protected Map<String, Object> getState() {
    return null;
  }

  @Override
  protected void setState(Map<String, Object> state) {

  }

  @Override
  public void restoreState(AutoScaling.Trigger old) {

  }

  @Override
  public void run() {

  }
}
