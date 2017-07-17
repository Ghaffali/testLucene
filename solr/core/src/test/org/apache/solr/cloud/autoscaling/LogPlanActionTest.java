package org.apache.solr.cloud.autoscaling;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.LogLevel;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.autoscaling.AutoScalingHandlerTest.createAutoScalingRequest;

/**
 * Test for {@link LogPlanAction}
 */
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG")
public class LogPlanActionTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final AtomicBoolean fired = new AtomicBoolean(false);
  private static final int NODE_COUNT = 3;
  private static CountDownLatch triggerFiredLatch = new CountDownLatch(1);
  private static final AtomicReference<Map> actionContextPropsRef = new AtomicReference<>();
  private static final AtomicReference<TriggerEvent> eventRef = new AtomicReference<>();

  public static class AssertingTriggerAction implements TriggerAction {

    @Override
    public String getName() {
      return null;
    }

    @Override
    public void process(TriggerEvent event, ActionContext context) {
      if (fired.compareAndSet(false, true)) {
        eventRef.set(event);
        actionContextPropsRef.set(context.getProperties());
        triggerFiredLatch.countDown();
      }
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void init(Map<String, String> args) {

    }
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(NODE_COUNT)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    CollectionAdminRequest.createCollection(CollectionAdminParams.SYSTEM_COLL, null, 1, 3)
        .process(cluster.getSolrClient());
  }

  @Test
  public void test() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '1s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'compute_plan', 'class' : 'solr.ComputePlanAction'}," +
        "{'name':'log_plan','class':'solr.LogPlanAction'}," +
        "{'name':'test','class':'" + AssertingTriggerAction.class.getName() + "'}]" +
        "}}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection("test",
        "conf",3, 2);
    create.setMaxShardsPerNode(3);
    create.process(solrClient);

    waitForState("Timed out waiting for replicas of new collection to be active",
        "test", clusterShape(3, 2));
    // stop non-overseer node
    NamedList<Object> overSeerStatus = cluster.getSolrClient().request(CollectionAdminRequest.getOverseerStatus());
    String overseerLeader = (String) overSeerStatus.get("leader");
    int nonOverseerLeaderIndex = 0;
    for (int i = 0; i < cluster.getJettySolrRunners().size(); i++) {
      JettySolrRunner jetty = cluster.getJettySolrRunner(i);
      if (!jetty.getNodeName().equals(overseerLeader)) {
        nonOverseerLeaderIndex = i;
      }
    }
    cluster.stopJettySolrRunner(nonOverseerLeaderIndex);
    cluster.waitForAllNodes(30);
    assertTrue("Trigger was not fired even after 5 seconds", triggerFiredLatch.await(5, TimeUnit.SECONDS));
    assertTrue(fired.get());
    Map context = actionContextPropsRef.get();
    assertNotNull(context);
    // make sure the event doc is committed
    cluster.getSolrClient().commit(CollectionAdminParams.SYSTEM_COLL);
    ModifiableSolrParams query = new ModifiableSolrParams();
    query.add(CommonParams.Q, "type:" + LogPlanAction.DOC_TYPE);
    QueryResponse resp = cluster.getSolrClient().query(CollectionAdminParams.SYSTEM_COLL, query);
    SolrDocumentList docs = resp.getResults();
    assertNotNull(docs);
    assertEquals("wrong number of events added to .system", 1, docs.size());
    SolrDocument doc = docs.get(0);
    assertEquals(LogPlanAction.class.getSimpleName(), doc.getFieldValue(LogPlanAction.SOURCE_FIELD));
    assertEquals(LogPlanAction.DOC_TYPE, doc.getFieldValue(CommonParams.TYPE));
    assertEquals("node_lost_trigger", doc.getFieldValue("event.source_s"));
    assertNotNull(doc.getFieldValue("event.time_l"));
    assertNotNull(doc.getFieldValue("timestamp"));
    assertNotNull(doc.getFieldValue("event.property.nodeName_s"));
    assertNotNull(doc.getFieldValue("event.property._enqueue_time__s"));
    assertNotNull(doc.getFieldValue("event_str"));
    assertNotNull(doc.getFieldValue("context_str"));
    assertEquals("NODELOST", doc.getFieldValue("event.type_s"));
    Collection<Object> vals = doc.getFieldValues("operations.params_ts");
    assertEquals(3, vals.size());
    for (Object val : vals) {
      assertTrue(val.toString(), String.valueOf(val).contains("action=MOVEREPLICA"));
    }
  }
}
