package org.apache.solr.cloud.autoscaling.sim;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    simulated = true;

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
      simProvider.simSetClusterProperties(clusterProperties);
      simProvider.simAddNodes(liveNodes);
      realState.getCollectionsMap().forEach((coll, dc) -> {
        dc.getSlices().forEach(slice -> {
          simProvider.setSliceProperties(coll, slice.getName(), slice.getProperties());
        });
      });
      replicas.forEach((n, infos) -> {
        infos.forEach((coll, shardInfos) -> {
          shardInfos.forEach((shard, replicaInfos) -> {
            replicaInfos.forEach(ri -> {
              simProvider.simAddReplica(n, ri);
            });
          });
        });
      });
      nodeValues.forEach((n, values) -> {
        simProvider.simSetNodeValues(n, values);
      });
      realState.getCollectionsMap().forEach((name, docColl) -> {
        simProvider.simSetCollectionProperties(name, docColl.getProperties());
      });
      ClusterState simState = simProvider.getClusterState();
      assertEquals(realState.getLiveNodes(), simState.getLiveNodes());
      assertEquals(realState.getCollectionsMap().keySet(), simState.getCollectionsMap().keySet());
      DocCollection realColl = realState.getCollection(CollectionAdminParams.SYSTEM_COLL);
      DocCollection simColl = simState.getCollection(CollectionAdminParams.SYSTEM_COLL);
      Map<String, Slice> realSlices = realColl.getSlicesMap();
      Map<String, Slice> simSlices = simColl.getSlicesMap();
      assertEquals(realSlices.keySet(), simSlices.keySet());
      realSlices.forEach((s, slice) -> {
        Slice sim = simSlices.get(s);
        for (Replica r : slice.getReplicas()) {
          Replica sr = sim.getReplica(r.getName());
          assertNotNull(sr);
          assertEquals(r, sr);
        }
      });
      clusterDataProvider = simProvider;
    } else {
      clusterDataProvider = realProvider;
    }
  }

  private void addNode() throws Exception {
    JettySolrRunner solr = cluster.startJettySolrRunner();
    String nodeId = solr.getNodeName();
    if (simulated) {
      ((SimClusterDataProvider)clusterDataProvider).simAddNode(nodeId);
    }
  }

  private void deleteNode() throws Exception {
    String nodeId = cluster.getJettySolrRunner(0).getNodeName();
    cluster.stopJettySolrRunner(0);
    if (simulated) {
      ((SimClusterDataProvider)clusterDataProvider).simRemoveNode(nodeId);
    }
  }

  private void setAutoScalingConfig(AutoScalingConfig cfg) throws Exception {
    cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getZkClient().setData(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH,
        Utils.toJSON(cfg), -1, true);
    if (simulated) {
      ((SimClusterDataProvider)clusterDataProvider).simSetAutoScalingConfig(cfg);
    }
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
