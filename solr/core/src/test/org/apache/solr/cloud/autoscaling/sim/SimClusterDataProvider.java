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
