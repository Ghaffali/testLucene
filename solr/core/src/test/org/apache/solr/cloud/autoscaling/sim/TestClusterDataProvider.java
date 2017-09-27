package org.apache.solr.cloud.autoscaling.sim;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.ClusterDataProvider;
import org.apache.solr.client.solrj.cloud.autoscaling.Preference;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.rule.ImplicitSnitch;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.Watcher;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TestClusterDataProvider extends SolrCloudTestCase {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static int NODE_COUNT = 3;
  private static boolean simulated;

  private static ClusterDataProvider clusterDataProvider;

  private static Collection<String> liveNodes;
  private static Map<String, Object> clusterProperties;
  private static AutoScalingConfig autoScalingConfig;
  private static Map<String, Map<String, Map<String, List<ReplicaInfo>>>> replicas;
  private static Map<String, Map<String, Object>> nodeValues;
  private static ClusterState realState;

  // set up a real cluster as the source of test data
  @BeforeClass
  public static void setupCluster() throws Exception {
    simulated = random().nextBoolean();
    //simulated = true;

    configureCluster(NODE_COUNT)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    CollectionAdminRequest.createCollection(CollectionAdminParams.SYSTEM_COLL, null, 1, 2, 0, 1)
        .process(cluster.getSolrClient());
    init();
  }

  private static void init() throws Exception {
    ClusterDataProvider realProvider = cluster.getJettySolrRunner(cluster.getJettySolrRunners().size() - 1).getCoreContainer()
        .getZkController().getSolrCloudDataProvider().getClusterDataProvider();
    liveNodes = realProvider.getLiveNodes();
    clusterProperties = realProvider.getClusterProperties();
    autoScalingConfig = realProvider.getAutoScalingConfig();
    replicas = new HashMap<>();
    nodeValues = new HashMap<>();
    realProvider.getLiveNodes().forEach(n -> {
      replicas.put(n, realProvider.getReplicaInfo(n, Collections.emptySet()));
      nodeValues.put(n, realProvider.getNodeValues(n, ImplicitSnitch.tags));
    });
    realState = realProvider.getClusterState();

    if (simulated) {
      // initialize simulated provider
      SimClusterDataProvider simProvider = new SimClusterDataProvider();
      simProvider.simSetAutoScalingConfig(autoScalingConfig);
      nodeValues.forEach((n, values) -> {
        simProvider.simSetNodeValues(n, values);
      });
      simProvider.simSetClusterState(realState);
      ClusterState simState = simProvider.getClusterState();
      assertClusterStateEquals(realState, simState);
      // test the other constructor
      ClusterDataProvider anotherProvider = new SimClusterDataProvider(realState, autoScalingConfig, nodeValues);
      assertClusterStateEquals(realState, anotherProvider.getClusterState());
      clusterDataProvider = simProvider;
    } else {
      clusterDataProvider = realProvider;
    }
  }

  private static void assertClusterStateEquals(ClusterState one, ClusterState two) {
    assertEquals(one.getLiveNodes(), two.getLiveNodes());
    assertEquals(one.getCollectionsMap().keySet(), two.getCollectionsMap().keySet());
    one.forEachCollection(oneColl -> {
      DocCollection twoColl = two.getCollection(oneColl.getName());
      Map<String, Slice> oneSlices = oneColl.getSlicesMap();
      Map<String, Slice> twoSlices = twoColl.getSlicesMap();
      assertEquals(oneSlices.keySet(), twoSlices.keySet());
      oneSlices.forEach((s, slice) -> {
        Slice sTwo = twoSlices.get(s);
        for (Replica oneReplica : slice.getReplicas()) {
          Replica twoReplica = sTwo.getReplica(oneReplica.getName());
          assertNotNull(twoReplica);
          assertEquals(oneReplica, twoReplica);
        }
      });
    });
  }

  private String addNode() throws Exception {
    JettySolrRunner solr = cluster.startJettySolrRunner();
    String nodeId = solr.getNodeName();
    if (simulated) {
      ((SimClusterDataProvider)clusterDataProvider).simAddNode(nodeId);
    }
    return nodeId;
  }

  private String deleteNode() throws Exception {
    String nodeId = cluster.getJettySolrRunner(0).getNodeName();
    cluster.stopJettySolrRunner(0);
    if (simulated) {
      ((SimClusterDataProvider)clusterDataProvider).simRemoveNode(nodeId);
    }
    return nodeId;
  }

  private void setAutoScalingConfig(AutoScalingConfig cfg) throws Exception {
    cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getZkClient().setData(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH,
        Utils.toJSON(cfg), -1, true);
    if (simulated) {
      ((SimClusterDataProvider)clusterDataProvider).simSetAutoScalingConfig(cfg);
    }
  }

  @Test
  public void testAddRemoveNode() throws Exception {
    Set<String> lastNodes = new HashSet<>(clusterDataProvider.getLiveNodes());
    String node = addNode();
    Thread.sleep(2000);
    assertFalse(lastNodes.contains(node));
    assertTrue(clusterDataProvider.getLiveNodes().contains(node));
    node = deleteNode();
    Thread.sleep(2000);
    assertTrue(lastNodes.contains(node));
    assertFalse(clusterDataProvider.getLiveNodes().contains(node));
  }

  @Test
  public void testAutoScalingConfig() throws Exception {
    final CountDownLatch triggered = new CountDownLatch(1);
    Watcher w = ev -> {
      if (triggered.getCount() == 0) {
        fail("already triggered once!");
      }
      triggered.countDown();
    };
    AutoScalingConfig cfg = clusterDataProvider.getAutoScalingConfig(w);
    assertEquals(autoScalingConfig, cfg);
    Preference p = new Preference(Collections.singletonMap("maximize", "freedisk"));
    cfg = cfg.withPolicy(cfg.getPolicy().withClusterPreferences(Collections.singletonList(p)));
    setAutoScalingConfig(cfg);
    if (!triggered.await(10, TimeUnit.SECONDS)) {
      fail("Watch should be triggered on update!");
    }
    AutoScalingConfig cfg1 = clusterDataProvider.getAutoScalingConfig(null);
    assertEquals(cfg, cfg1);

    // restore
    setAutoScalingConfig(autoScalingConfig);
    cfg1 = clusterDataProvider.getAutoScalingConfig(null);
    assertEquals(autoScalingConfig, cfg1);
  }
}
