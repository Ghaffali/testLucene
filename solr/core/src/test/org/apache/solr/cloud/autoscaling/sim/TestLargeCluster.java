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

import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventProcessorStage;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.autoscaling.ActionContext;
import org.apache.solr.cloud.autoscaling.ComputePlanAction;
import org.apache.solr.cloud.autoscaling.ExecutePlanAction;
import org.apache.solr.cloud.autoscaling.TriggerActionBase;
import org.apache.solr.cloud.autoscaling.TriggerEvent;
import org.apache.solr.cloud.autoscaling.TriggerListenerBase;
import org.apache.solr.cloud.autoscaling.CapturedEvent;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.LogLevel;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.autoscaling.AutoScalingHandlerTest.createAutoScalingRequest;

/**
 *
 */
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG")
public class TestLargeCluster extends SimSolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final int SPEED = 50;

  public static final int NUM_NODES = 500;

  static Map<String, List<CapturedEvent>> listenerEvents = new ConcurrentHashMap<>();
  static AtomicInteger triggerFiredCount = new AtomicInteger();
  static int waitForSeconds;

  @BeforeClass
  public static void setupCluster() throws Exception {
    cluster = SimCloudManager.createCluster(NUM_NODES, TimeSource.get("simTime:" + SPEED));
  }

  @Before
  public void setupTest() throws Exception {
    // clear any persisted auto scaling configuration
    cluster.getDistribStateManager().setData(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH,
        "{}".getBytes(StandardCharsets.UTF_8), -1);
    cluster.simClearSystemCollection();
    cluster.getSimClusterStateProvider().simDeleteAllCollections();

    waitForSeconds = 1 + random().nextInt(3);
    triggerFiredCount.set(0);
    listenerEvents.clear();
    // clear any events or markers
    // todo: consider the impact of such cleanup on regular cluster restarts
    removeChildren(ZkStateReader.SOLR_AUTOSCALING_EVENTS_PATH);
    removeChildren(ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH);
    removeChildren(ZkStateReader.SOLR_AUTOSCALING_NODE_LOST_PATH);
    removeChildren(ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH);
    while (cluster.getClusterStateProvider().getLiveNodes().size() < NUM_NODES) {
      // perhaps a test stopped a node but didn't start it back
      // lets start a node
      cluster.simAddNode();
    }
  }

  public static class TestTriggerListener extends TriggerListenerBase {
    @Override
    public void init(SolrCloudManager cloudManager, AutoScalingConfig.TriggerListenerConfig config) {
      super.init(cloudManager, config);
    }

    @Override
    public synchronized void onEvent(TriggerEvent event, TriggerEventProcessorStage stage, String actionName,
                                     ActionContext context, Throwable error, String message) {
      List<CapturedEvent> lst = listenerEvents.computeIfAbsent(config.name, s -> new ArrayList<>());
      lst.add(new CapturedEvent(cluster.getTimeSource().getTime(), context, config, stage, actionName, event, message));
    }
  }

  public static class TestTriggerAction extends TriggerActionBase {
    @Override
    public void process(TriggerEvent event, ActionContext context) throws Exception {
      triggerFiredCount.incrementAndGet();
    }
  }

  @Test
  public void testBasic() throws Exception {
    assertEquals(NUM_NODES, cluster.getClusterStateProvider().getLiveNodes().size());
    SolrClient solrClient = cluster.simGetSolrClient();
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'enabled' : true," +
        "'actions' : [" +
        "{'name':'compute','class':'" + ComputePlanAction.class.getName() + "'}," +
        "{'name':'execute','class':'" + ExecutePlanAction.class.getName() + "'}," +
        "{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}" +
        "]" +
        "}}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'foo'," +
        "'trigger' : 'node_lost_trigger'," +
        "'stage' : ['STARTED','ABORTED','SUCCEEDED', 'FAILED']," +
        "'beforeAction' : ['compute', 'execute']," +
        "'afterAction' : ['compute', 'execute']," +
        "'class' : '" + TestTriggerListener.class.getName() + "'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    cluster.getTimeSource().sleep(5000);

    // pick a few random nodes
    List<String> nodes = new ArrayList<>();
    int limit = 100;
    for (String node : cluster.getClusterStateProvider().getLiveNodes()) {
      nodes.add(node);
    }
    Collections.shuffle(nodes, random());
    String collectionName = "testBasic";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf", 10, 5, 5, 5);
    create.setMaxShardsPerNode(1);
    create.setCreateNodeSet(String.join(",", nodes));
    create.process(solrClient);

    waitForState("Timed out waiting for replicas of new collection to be active",
        collectionName, clusterShape(10, 15));

    // kill off a number of nodes
    for (int i = 0; i < 20; i++) {
      cluster.simRemoveNode(nodes.get(i));
    }

    waitForState("Timed out waiting for replicas of new collection to be active",
        collectionName, clusterShape(10, 15));
    log.info(cluster.simGetSystemCollection().toString());
  }

  @Test
  public void testSearchRate() throws Exception {

  }
}
