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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.AtomicDouble;
import org.apache.solr.client.solrj.cloud.autoscaling.Policy;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.SolrClientDataProvider;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.params.AutoScalingParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.metrics.SolrCoreMetricManager;
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
  private final String handler;
  private final String collection;
  private final String shard;
  private final String node;
  private final double rate;
  private long lastEvent;

  public SearchRateTrigger(String name, Map<String, Object> properties, CoreContainer container) {
    super(name, properties, container.getResourceLoader(), container.getZkController().getZkClient());
    this.container = container;
    this.timeSource = TimeSource.CURRENT_TIME;
    this.cloudSolrClient = new CloudSolrClient.Builder()
        .withClusterStateProvider(new ZkClientClusterStateProvider(container.getZkController().getZkStateReader()))
        .build();
    // parse config options
    collection = (String)properties.getOrDefault(AutoScalingParams.COLLECTION, Policy.ANY);
    shard = (String)properties.getOrDefault(AutoScalingParams.SHARD, Policy.ANY);
    node = (String)properties.getOrDefault(AutoScalingParams.NODE, Policy.ANY);
    handler = (String)properties.getOrDefault(AutoScalingParams.HANDLER, "/select");
    lastEvent = 0;

    if (properties.get("rate") == null) {
      throw new IllegalArgumentException("No 'rate' specified in configuration");
    }
    String rateString = String.valueOf(properties.get("rate"));
    try {
      rate = Double.parseDouble(rateString);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid 'rate' configuration value: '" + rateString + "'", e);
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
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
    long now = timeSource.getTime();
    if (TimeUnit.SECONDS.convert(now - lastEvent, TimeUnit.NANOSECONDS) < getWaitForSecond()) {
      return;
    }
    AutoScaling.TriggerEventProcessor processor = processorRef.get();
    if (processor == null) {
      return;
    }

    SolrClientDataProvider dataProvider = new SolrClientDataProvider(cloudSolrClient);

    Map<String, Map<String, List<ReplicaInfo>>> collectionRates = new HashMap<>();
    Map<String, AtomicDouble> nodeRates = new HashMap<>();
    for (String node : dataProvider.getNodes()) {
      Map<String, ReplicaInfo> metricTags = new HashMap<>();
      // coll, shard, replica
      Map<String, Map<String, List<ReplicaInfo>>> infos = dataProvider.getReplicaInfo(node, Collections.emptyList());
      infos.forEach((coll, shards) -> {
        shards.forEach((sh, replicas) -> {
          replicas.forEach(replica -> {
            // we have to translate to the metrics registry name, which uses "_replica_nN" as suffix
            String replicaName = SolrCoreMetricManager.parseReplicaName(coll, replica.getCore());
            if (replicaName == null) { // should never happen???
              replicaName = replica.getName(); // which is actually coreNode name...
            }
            String registry = SolrCoreMetricManager.createRegistryName(true, coll, sh, replicaName, null);
            String tag = "metrics:" + registry
                + ":QUERY." + handler + ".requestTimes:1minRate";
            metricTags.put(tag, replica);
          });
        });
      });
      Map<String, Object> rates = dataProvider.getNodeValues(node, metricTags.keySet());
      rates.forEach((tag, rate) -> {
        ReplicaInfo info = metricTags.get(tag);
        if (info == null) {
          log.warn("Missing replica info for response tag " + tag);
        } else {
          Map<String, List<ReplicaInfo>> perCollection = collectionRates.computeIfAbsent(info.getCollection(), s -> new HashMap<>());
          List<ReplicaInfo> perShard = perCollection.computeIfAbsent(info.getShard(), s -> new ArrayList<>());
          info.getVariables().put(AutoScalingParams.RATE, rate);
          perShard.add(info);
          AtomicDouble perNode = nodeRates.computeIfAbsent(node, s -> new AtomicDouble());
          perNode.addAndGet((Double)rate);
        }
      });
    }
    // check for exceeded rates
    Map<String, Double> hotNodes = nodeRates.entrySet().stream()
        .filter(entry -> entry.getValue().get() > rate)
        .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue().get()));

    Map<String, Map<String, Double>> hotShards = new HashMap<>();
    List<ReplicaInfo> hotReplicas = new ArrayList<>();
    collectionRates.forEach((coll, shardRates) -> {
      shardRates.forEach((sh, replicaRates) -> {
        double shardRate = replicaRates.stream()
            .map(r -> {
              if ((Double)r.getVariable(AutoScalingParams.RATE) > rate) {
                hotReplicas.add(r);
              }
              return r;
            })
            .mapToDouble(r -> (Double)r.getVariable(AutoScalingParams.RATE)).sum();
        if (shardRate > rate) {
          hotShards.computeIfAbsent(coll, s -> new HashMap<>()).put(sh, shardRate);
        }
      });
    });

    Map<String, Double> hotCollections = new HashMap<>();
    collectionRates.forEach((coll, shardRates) -> {
      double total = shardRates.entrySet().stream()
          .mapToDouble(e -> e.getValue().stream()
              .mapToDouble(r -> (Double)r.getVariable(AutoScalingParams.RATE)).sum()).sum();
      if (total > rate) {
        hotCollections.put(coll, total);
      }
    });

    if (hotCollections.isEmpty() && hotShards.isEmpty() && hotReplicas.isEmpty() && hotNodes.isEmpty()) {
      return;
    }

    // generate event

    if (processor.process(new SearchRateEvent(getName(), now, hotNodes, hotCollections, hotShards, hotReplicas))) {
      lastEvent = now;
    }
  }

  public static class SearchRateEvent extends TriggerEvent {
    public SearchRateEvent(String source, long eventTime, Map<String, Double> hotNodes,
                           Map<String, Double> hotCollections,
                           Map<String, Map<String, Double>> hotShards, List<ReplicaInfo> hotReplicas) {
      super(TriggerEventType.SEARCHRATE, source, eventTime, null);
      properties.put(AutoScalingParams.COLLECTION, hotCollections);
      properties.put(AutoScalingParams.SHARD, hotShards);
      properties.put(AutoScalingParams.REPLICA, hotReplicas);
      properties.put(AutoScalingParams.NODE, hotNodes);
    }
  }
}