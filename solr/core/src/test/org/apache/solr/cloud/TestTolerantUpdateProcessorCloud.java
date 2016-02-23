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
package org.apache.solr.cloud;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressSysoutChecks;
import org.apache.solr.SolrTestCaseJ4;
import static org.apache.solr.SolrTestCaseJ4.params;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettyConfig.Builder;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.util.RevertDefaultThreadHandlerRule;

import org.junit.ClassRule;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test of TolerantUpdateProcessor using a MiniSolrCloud.  Updates (that include failures which 
 * should be tolerated) are explicitly tested against various initial nodes to confirm correct 
 * behavior regardless of routing.
 *
 * <p>
 * <b>NOTE:</b> This test sets up a static instance of MiniSolrCloud with a single collection 
 * and several clients pointed at specific nodes. These are all re-used across multiple test methods, 
 * and assumesthat the state of the cluster is healthy.
 * </p>
 *
 * nocommit: test deletions that fail (and are ignored because of maxErrors) as well...
 *  - nocommit: DBQ with malformed query
 *  - nocommit: delete by id with incorrect version (optimistic concurrency fail)
 *
 * nocommit: what about shard splitting and "sub shard leaders" ? ...
 * (no idea if/how that affects things, but i notice lots of logic in DistributedUpdateProcessor along 
 * the lines of "if (isLeader || isSubShardLeader)" and "if (!isLeader) { if (subShardLeader) {..." 
 * which makes me worry that we may need explict testing of "tolerant" behavior when updates are routed 
 * to subshards and then fail?
 *
 * nocommit: once these tests are passing reliably, we should also have a fully randomized sibling test...
 * - randomized # nodes, shards, replicas
 * - random updates contain rand # of docs with rand # failures to a random client
 */
@SuppressSysoutChecks(bugUrl = "Solr logs to JUL")
public class TestTolerantUpdateProcessorCloud extends LuceneTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int NUM_SHARDS = 2; 
  private static final int REPLICATION_FACTOR = 2; 
  private static final int NUM_SERVERS = 5; 
  
  private static final String COLLECTION_NAME = "test_col";
  
  private static MiniSolrCloudCluster SOLR_CLUSTER;

  /** A basic client for operations at the cloud level, default collection will be set */
  private static CloudSolrClient CLOUD_CLIENT;

  /** A client for talking directly to the leader of shard1 */
  private static HttpSolrClient S_ONE_LEADER_CLIENT;
  
  /** A client for talking directly to the leader of shard2 */
  private static HttpSolrClient S_TWO_LEADER_CLIENT;

  /** A client for talking directly to a passive replica of shard1 */
  private static HttpSolrClient S_ONE_NON_LEADER_CLIENT;
  
  /** A client for talking directly to a passive replica of shard2 */
  private static HttpSolrClient S_TWO_NON_LEADER_CLIENT;

  /** A client for talking directly to a node that has no piece of the collection */
  private static HttpSolrClient NO_COLLECTION_CLIENT;
  
  /** id field doc routing prefix for shard1 */
  private static final String S_ONE_PRE = "abc!";
  
  /** id field doc routing prefix for shard2 */
  private static final String S_TWO_PRE = "XYZ!";
  
  
  @Rule
  public TestRule solrTestRules = RuleChain.outerRule(new SystemPropertiesRestoreRule());
  
  @ClassRule
  public static TestRule solrClassRules = RuleChain.outerRule
    (new SystemPropertiesRestoreRule()).around(new RevertDefaultThreadHandlerRule());

  @BeforeClass
  private static void createMiniSolrCloudCluster() throws Exception {
    // nocommit: should we just be subclassing SolrTestCaseJ4 and get this for free?
    SolrTestCaseJ4.chooseMPForMP();
    
    Builder jettyConfig = JettyConfig.builder();
    jettyConfig.waitForLoadingCoresToFinish(null);
    SOLR_CLUSTER = new MiniSolrCloudCluster(NUM_SERVERS, createTempDir(), jettyConfig.build());
    
    String configName = "solrCloudCollectionConfig";
    File configDir = new File(SolrTestCaseJ4.TEST_HOME() + File.separator + "collection1" + File.separator + "conf");
    SOLR_CLUSTER.uploadConfigDir(configDir, configName);

    SolrTestCaseJ4.newRandomConfig();
    
    Map<String, String> collectionProperties = new HashMap<>();
    collectionProperties.put("config", "solrconfig-distrib-update-processor-chains.xml");
    collectionProperties.put("schema", "schema15.xml"); // string id for doc routing prefix

    assertNotNull(SOLR_CLUSTER.createCollection(COLLECTION_NAME, NUM_SHARDS, REPLICATION_FACTOR,
                                                configName, null, null, collectionProperties));
    
    CLOUD_CLIENT = SOLR_CLUSTER.getSolrClient();
    CLOUD_CLIENT.setDefaultCollection(COLLECTION_NAME);
    
    ZkStateReader zkStateReader = CLOUD_CLIENT.getZkStateReader();
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(COLLECTION_NAME, zkStateReader, true, true, 330);


    // really hackish way to get a URL for specific nodes based on shard/replica hosting
    // inspired by TestMiniSolrCloudCluster
    HashMap<String, String> urlMap = new HashMap<>();
    for (JettySolrRunner jetty : SOLR_CLUSTER.getJettySolrRunners()) {
      URL jettyURL = jetty.getBaseUrl();
      String nodeKey = jettyURL.getHost() + ":" + jettyURL.getPort() + jettyURL.getPath().replace("/","_");
      urlMap.put(nodeKey, jettyURL.toString());
    }
    zkStateReader.updateClusterState();
    ClusterState clusterState = zkStateReader.getClusterState();
    for (Slice slice : clusterState.getSlices(COLLECTION_NAME)) {
      String shardName = slice.getName();
      Replica leader = slice.getLeader();
      assertNotNull("slice has null leader: " + slice.toString(), leader);
      assertNotNull("slice leader has null node name: " + slice.toString(), leader.getNodeName());
      String leaderUrl = urlMap.remove(leader.getNodeName());
      assertNotNull("could not find URL for " + shardName + " leader: " + leader.getNodeName(),
                    leaderUrl);
      assertEquals("expected two total replicas for: " + slice.getName(),
                   2, slice.getReplicas().size());
      
      String passiveUrl = null;
      
      for (Replica replica : slice.getReplicas()) {
        if ( ! replica.equals(leader)) {
          passiveUrl = urlMap.remove(replica.getNodeName());
          assertNotNull("could not find URL for " + shardName + " replica: " + replica.getNodeName(),
                        passiveUrl);
        }
      }
      assertNotNull("could not find URL for " + shardName + " replica", passiveUrl);

      if (shardName.equals("shard1")) {
        S_ONE_LEADER_CLIENT = new HttpSolrClient(leaderUrl + "/" + COLLECTION_NAME + "/");
        S_ONE_NON_LEADER_CLIENT = new HttpSolrClient(passiveUrl + "/" + COLLECTION_NAME + "/");
      } else if (shardName.equals("shard2")) {
        S_TWO_LEADER_CLIENT = new HttpSolrClient(leaderUrl + "/" + COLLECTION_NAME + "/");
        S_TWO_NON_LEADER_CLIENT = new HttpSolrClient(passiveUrl + "/" + COLLECTION_NAME + "/");
      } else {
        fail("unexpected shard: " + shardName);
      }
    }
    assertEquals("Should be exactly one server left (nost hosting either shard)", 1, urlMap.size());
    NO_COLLECTION_CLIENT = new HttpSolrClient(urlMap.values().iterator().next() +
                                              "/" + COLLECTION_NAME + "/");
    
    assertNotNull(S_ONE_LEADER_CLIENT);
    assertNotNull(S_TWO_LEADER_CLIENT);
    assertNotNull(S_ONE_NON_LEADER_CLIENT);
    assertNotNull(S_TWO_NON_LEADER_CLIENT);
    assertNotNull(NO_COLLECTION_CLIENT);

    // sanity check that our S_ONE_PRE & S_TWO_PRE really do map to shard1 & shard2 with default routing
    assertEquals(0, CLOUD_CLIENT.add(doc(f("id", S_ONE_PRE + random().nextInt()),
                                         f("expected_shard_s", "shard1"))).getStatus());
    assertEquals(0, CLOUD_CLIENT.add(doc(f("id", S_TWO_PRE + random().nextInt()),
                                         f("expected_shard_s", "shard2"))).getStatus());
    assertEquals(0, CLOUD_CLIENT.commit().getStatus());
    SolrDocumentList docs = CLOUD_CLIENT.query(params("q", "*:*",
                                                      "fl","id,expected_shard_s,[shard]")).getResults();
    assertEquals(2, docs.getNumFound());
    assertEquals(2, docs.size());
    for (SolrDocument doc : docs) {
      String expected = COLLECTION_NAME + "_" + doc.getFirstValue("expected_shard_s") + "_replica";
      String docShard = doc.getFirstValue("[shard]").toString();
      assertTrue("shard routing prefixes don't seem to be aligned anymore, " +
                 "did someone change the default routing rules? " +
                 "and/or the the default core name rules? " +
                 "and/or the numShards used by this test? ... " +
                 "couldn't find " + expected + " as substring of [shard] == '" + docShard +
                 "' ... for docId == " + doc.getFirstValue("id"),
                 docShard.contains(expected));
    }
  }
  
  @AfterClass
  private static void shutdownMiniSolrCloudCluster() throws Exception {
    SOLR_CLUSTER.shutdown();

    // nocommit: should we just be subclassing SolrTestCaseJ4 and get this for free?
    SolrTestCaseJ4.unchooseMPForMP();
  }
  
  @Before
  private void clearIndex() throws Exception {
    assertEquals(0, CLOUD_CLIENT.deleteByQuery("*:*").getStatus());
    assertEquals(0, CLOUD_CLIENT.commit().getStatus());
  }

  public void testSanity() throws Exception {
    
    // verify some basic sanity checking of indexing & querying across the collection
    
    assertEquals(0, CLOUD_CLIENT.add(doc(f("id", S_ONE_PRE + "1"),
                                         f("foo_i", 42))).getStatus());
    assertEquals(0, CLOUD_CLIENT.add(doc(f("id", S_TWO_PRE + "2"),
                                         f("foo_i", 66))).getStatus());
    assertEquals(0, CLOUD_CLIENT.commit().getStatus());

    for (SolrClient c : Arrays.asList(S_ONE_LEADER_CLIENT, S_TWO_LEADER_CLIENT,
                                      S_ONE_NON_LEADER_CLIENT, S_TWO_NON_LEADER_CLIENT,
                                      NO_COLLECTION_CLIENT, CLOUD_CLIENT)) {
      assertQueryDocIds(c, true, S_ONE_PRE + "1",  S_TWO_PRE + "2");
      assertQueryDocIds(c, false, "42");
    }


    
  }

  public void testVariousUpdatesViaCloudClient() throws Exception {
    testVariousUpdates(CLOUD_CLIENT);
  }
  public void testVariousUpdatesViaShard1LeaderClient() throws Exception {
    testVariousUpdates(S_ONE_LEADER_CLIENT);
  }
  public void testVariousUpdatesViaShard2LeaderClient() throws Exception {
    testVariousUpdates(S_TWO_LEADER_CLIENT);
  }
  public void testVariousUpdatesViaShard1NonLeaderClient() throws Exception {
    testVariousUpdates(S_ONE_NON_LEADER_CLIENT);
  }
  public void testVariousUpdatesViaShard2NonLeaderClient() throws Exception {
    testVariousUpdates(S_TWO_NON_LEADER_CLIENT);
  }
  public void testVariousUpdatesViaNoCollectionClient() throws Exception {
    testVariousUpdates(NO_COLLECTION_CLIENT);
  }

  protected static void testVariousUpdates(SolrClient client) throws Exception {
    assertNotNull("client not initialized", client);
    
    UpdateResponse rsp = null;

    // 2 docs that are both on shard1, the first one should fail
    
    rsp = update(params("update.chain", "tolerant-chain-max-errors-10",
                        "commit", "true"),
                 doc(f("id", S_ONE_PRE + "42"), f("foo_i", "bogus_value")),
                 doc(f("id", S_ONE_PRE + "666"), f("foo_i", "1976"))).process(client);
    
    assertEquals(0, rsp.getStatus());
    assertUpdateTolerantErrors("single shard, 1st doc should fail", rsp, S_ONE_PRE + "42");
    assertEquals(0, client.commit().getStatus());
    assertQueryDocIds(client, false, S_ONE_PRE + "42");
    assertQueryDocIds(client, true, S_ONE_PRE + "666");
           
    // 2 docs that are both on shard1, the second one should fail
    
    rsp = update(params("update.chain", "tolerant-chain-max-errors-10",
                        "commit", "true"),
                 doc(f("id", S_ONE_PRE + "55"), f("foo_i", "1976")),
                 doc(f("id", S_ONE_PRE + "77"), f("foo_i", "bogus_val"))).process(client);
    
    assertEquals(0, rsp.getStatus());
    assertUpdateTolerantErrors("single shard, 2nd doc should fail", rsp, S_ONE_PRE + "77");
    assertQueryDocIds(client, false, S_ONE_PRE + "77");
    assertQueryDocIds(client, true, S_ONE_PRE + "666", S_ONE_PRE + "55");

    // clean slate
    assertEquals(0, client.deleteByQuery("*:*").getStatus());

    // 2 docs on 2 diff shards, first of which should fail
    
    rsp = update(params("update.chain", "tolerant-chain-max-errors-10",
                        "commit", "true"),
                 doc(f("id", S_ONE_PRE + "42"), f("foo_i", "bogus_value")),
                 doc(f("id", S_TWO_PRE + "666"), f("foo_i", "1976"))).process(client);
    
    assertEquals(0, rsp.getStatus());
    assertUpdateTolerantErrors("two shards, 1st doc should fail", rsp, S_ONE_PRE + "42");
    assertEquals(0, client.commit().getStatus());
    assertQueryDocIds(client, false, S_ONE_PRE + "42");
    assertQueryDocIds(client, true, S_TWO_PRE + "666");
    
    // 2 docs on 2 diff shards, second of which should fail

    rsp = update(params("update.chain", "tolerant-chain-max-errors-10",
                        "commit", "true"),
                 doc(f("id", S_ONE_PRE + "55"), f("foo_i", "1976")),
                 doc(f("id", S_TWO_PRE + "77"), f("foo_i", "bogus_val"))).process(client);
    
    assertEquals(0, rsp.getStatus());
    assertUpdateTolerantErrors("two shards, 2nd doc should fail", rsp, S_TWO_PRE + "77");
    assertQueryDocIds(client, false, S_TWO_PRE + "77");
    assertQueryDocIds(client, true, S_TWO_PRE + "666", S_ONE_PRE + "55");

    // clean slate
    assertEquals(0, client.deleteByQuery("*:*").getStatus());

    // many docs from diff shards, 1 from each shard should fail
    
    rsp = update(params("update.chain", "tolerant-chain-max-errors-10",
                        "commit", "true"),
                 doc(f("id", S_ONE_PRE + "11")),
                 doc(f("id", S_TWO_PRE + "21")),
                 doc(f("id", S_ONE_PRE + "12")),
                 doc(f("id", S_TWO_PRE + "22"), f("foo_i", "bogus_val")),
                 doc(f("id", S_ONE_PRE + "13")),
                 doc(f("id", S_TWO_PRE + "23")),
                 doc(f("id", S_ONE_PRE + "14")),
                 doc(f("id", S_TWO_PRE + "24")),
                 doc(f("id", S_ONE_PRE + "15"), f("foo_i", "bogus_val")),
                 doc(f("id", S_TWO_PRE + "25")),
                 doc(f("id", S_ONE_PRE + "16")),
                 doc(f("id", S_TWO_PRE + "26"))).process(client);
    
    assertEquals(0, rsp.getStatus());
    assertUpdateTolerantErrors("many docs, 1 from each shard should fail", rsp,
                               S_ONE_PRE + "15",
                               S_TWO_PRE + "22");
    assertQueryDocIds(client, false, S_TWO_PRE + "22", S_ONE_PRE + "15");
    assertQueryDocIds(client, true,
                      S_ONE_PRE + "11", S_TWO_PRE + "21", S_ONE_PRE + "12",
                      S_ONE_PRE + "13", S_TWO_PRE + "23", S_ONE_PRE + "14", S_TWO_PRE + "24",
                      S_TWO_PRE + "25", S_ONE_PRE + "16", S_TWO_PRE + "26");

    // clean slate
    assertEquals(0, client.deleteByQuery("*:*").getStatus());
    
    // many docs from diff shards, more then 10 (total) should fail

    try {
      rsp = update(params("update.chain", "tolerant-chain-max-errors-10",
                          "commit", "true"),
                   doc(f("id", S_ONE_PRE + "11")),
                   doc(f("id", S_TWO_PRE + "21"), f("foo_i", "bogus_val")),
                   doc(f("id", S_ONE_PRE + "12")),
                   doc(f("id", S_TWO_PRE + "22"), f("foo_i", "bogus_val")),
                   doc(f("id", S_ONE_PRE + "13")),
                   doc(f("id", S_TWO_PRE + "23"), f("foo_i", "bogus_val")),
                   doc(f("id", S_ONE_PRE + "14"), f("foo_i", "bogus_val")),
                   doc(f("id", S_TWO_PRE + "24")),
                   doc(f("id", S_ONE_PRE + "15"), f("foo_i", "bogus_val")),
                   doc(f("id", S_TWO_PRE + "25")),
                   doc(f("id", S_ONE_PRE + "16"), f("foo_i", "bogus_val")),
                   doc(f("id", S_TWO_PRE + "26"), f("foo_i", "bogus_val")),
                   doc(f("id", S_ONE_PRE + "17")),
                   doc(f("id", S_TWO_PRE + "27")),
                   doc(f("id", S_ONE_PRE + "18"), f("foo_i", "bogus_val")),
                   doc(f("id", S_TWO_PRE + "28"), f("foo_i", "bogus_val")),
                   doc(f("id", S_ONE_PRE + "19"), f("foo_i", "bogus_val")),
                   doc(f("id", S_TWO_PRE + "29"), f("foo_i", "bogus_val")),
                   doc(f("id", S_ONE_PRE + "10")), // may be skipped, more then 10 fails
                   doc(f("id", S_TWO_PRE + "20"))  // may be skipped, more then 10 fails
                   ).process(client);

      // nocommit: should this really be a top level exception?
      // nocommit: or should it be an HTTP:200 with the details of what faild in the body?
      
      fail("did not get a top level exception when more then 10 docs failed: " + rsp.toString());
    } catch (SolrException e) {
      // we can't make any reliable assertions about the error message, because
      // it varies based on how the request was routed
      // nocommit: can we tighten this any more? substring check?
      assertEquals("not the type of error we were expecting ("+e.code()+"): " + e.toString(),
                   // NOTE: we always expect a 400 because we know that's what we would get from these types of errors
                   // on a single node setup -- a 5xx type error isn't something we should have triggered
                   400, e.code());

      // nocommit: is there a way to inspect the response body anyway?
      // nocommit: look for the correct "errors" ?  .... check e's metatata
    }
    assertEquals(0, client.commit().getStatus()); // need to force since update didn't finish
    assertQueryDocIds(client, false
                      // explicitly failed
                      , S_TWO_PRE + "21", S_TWO_PRE + "22", S_TWO_PRE + "23", S_ONE_PRE + "14"
                      , S_ONE_PRE + "15", S_ONE_PRE + "16", S_TWO_PRE + "26", S_ONE_PRE + "18"
                      , S_TWO_PRE + "28", S_ONE_PRE + "19", S_TWO_PRE + "29"
                      //
                      // // we can't assert for sure these docs were skipped
                      // // depending on shard we hit, they may have been added async before errors were exceeded
                      // , S_ONE_PRE + "10", S_TWO_PRE + "20" // skipped
                      );
    assertQueryDocIds(client, true,
                      S_ONE_PRE + "11", S_ONE_PRE + "12", S_ONE_PRE + "13", S_TWO_PRE + "24",
                      S_TWO_PRE + "25", S_ONE_PRE + "17", S_TWO_PRE + "27");
    
    // clean slate
    assertEquals(0, client.deleteByQuery("*:*").getStatus());
    
    // many docs from diff shards, more then 10 from a single shard (two) should fail

    try {
      ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>(30);
      docs.add(doc(f("id", S_ONE_PRE + "z")));
      docs.add(doc(f("id", S_TWO_PRE + "z")));
      docs.add(doc(f("id", S_ONE_PRE + "y")));
      docs.add(doc(f("id", S_TWO_PRE + "y")));
      for (int i = 0; i < 11; i++) {
        docs.add(doc(f("id", S_ONE_PRE + i)));
        docs.add(doc(f("id", S_TWO_PRE + i), f("foo_i", "bogus_val")));
      }
      docs.add(doc(f("id", S_ONE_PRE + "x"))); // may be skipped, more then 10 fails
      docs.add(doc(f("id", S_TWO_PRE + "x"))); // may be skipped, more then 10 fails
          
      rsp = update(params("update.chain", "tolerant-chain-max-errors-10",
                          "commit", "true"),
                   docs.toArray(new SolrInputDocument[docs.size()])).process(client);
      
      // nocommit: should this really be a top level exception?
      // nocommit: or should it be an HTTP:200 with the details of what faild in the body?
      
      fail("did not get a top level exception when more then 10 docs failed: " + rsp.toString());
    } catch (SolrException e) {
      // we can't make any reliable assertions about the error message, because
      // it varies based on how the request was routed
      // nocommit: can we tighten this any more? substring check?
      assertEquals("not the type of error we were expecting ("+e.code()+"): " + e.toString(),
                   // NOTE: we always expect a 400 because we know that's what we would get from these types of errors
                   // on a single node setup -- a 5xx type error isn't something we should have triggered
                   400, e.code());

      // nocommit: is there a way to inspect the response body anyway?
      // nocommit: look for the correct "errors" ?  .... check e's metatata
    }
    assertEquals(0, client.commit().getStatus()); // need to force since update didn't finish
    assertQueryDocIds(client, true
                      , S_ONE_PRE + "z", S_ONE_PRE + "y", S_TWO_PRE + "z", S_TWO_PRE + "y" // first
                      //
                      , S_ONE_PRE + "0", S_ONE_PRE + "1", S_ONE_PRE + "2", S_ONE_PRE + "3", S_ONE_PRE + "4"
                      , S_ONE_PRE + "5", S_ONE_PRE + "6", S_ONE_PRE + "7", S_ONE_PRE + "8", S_ONE_PRE + "9"
                      );
    assertQueryDocIds(client, false
                      // explicitly failed
                      , S_TWO_PRE + "0", S_TWO_PRE + "1", S_TWO_PRE + "2", S_TWO_PRE + "3", S_TWO_PRE + "4"
                      , S_TWO_PRE + "5", S_TWO_PRE + "6", S_TWO_PRE + "7", S_TWO_PRE + "8", S_TWO_PRE + "9"
                      //
                      // // we can't assert for sure these docs were skipped
                      // // depending on shard we hit, they may have been added async before errors were exceeded
                      // , S_ONE_PRE + "x", S_TWO_PRE + "x", // skipped
                      );

  }

  // nocommit: refactor into multiple methods, some of which can check tolerant deletions as well?
  public static void assertUpdateTolerantErrors(String assertionMsgPrefix,
                                                UpdateResponse response,
                                                String... errorIdsExpected) {

    @SuppressWarnings("unchecked")
    SimpleOrderedMap<Object> errors = (SimpleOrderedMap<Object>) response.getResponseHeader().get("errors");
    assertNotNull(assertionMsgPrefix + ": Null errors: " + response.toString(), errors);
    assertEquals(assertionMsgPrefix + ": Num error ids: " + errors.toString(),
                 errorIdsExpected.length, errors.size());
    for (String id : errorIdsExpected) {
      assertNotNull(assertionMsgPrefix + ": Id " + id + " not found in errors: " + errors.toString(),
                    errors.get(id));
    }
    
    assertEquals(assertionMsgPrefix + ": numErrors: " + response.toString(),
                 errorIdsExpected.length, response.getResponseHeader().get("numErrors"));
  }

  /** 
   * Asserts that the specified document ids do/do-not exist in the index, using both the specified client, 
   * and the CLOUD_CLIENT 
   */
  public static void assertQueryDocIds(SolrClient client, boolean shouldExist, String... ids) throws Exception {
    for (String id : ids) {
      assertEquals(client.toString() + " should " + (shouldExist ? "" : "not ") + "find id: " + id,
                   (shouldExist ? 1 : 0),
                   CLOUD_CLIENT.query(params("q", "{!term f=id}" + id)).getResults().getNumFound());
    }
    if (! CLOUD_CLIENT.equals(client) ) {
      assertQueryDocIds(CLOUD_CLIENT, shouldExist, ids);
    }
  }
  
  public static UpdateRequest update(SolrParams params, SolrInputDocument... docs) {
    UpdateRequest r = new UpdateRequest();
    r.setParams(new ModifiableSolrParams(params));
    r.add(Arrays.asList(docs));
    return r;
  }

  
  public static SolrInputDocument doc(SolrInputField... fields) {
    SolrInputDocument doc = new SolrInputDocument();
    for (SolrInputField f : fields) {
      doc.put(f.getName(), f);
    }
    return doc;
  }
  
  public static SolrInputField f(String fieldName, Object... values) {
    SolrInputField f = new SolrInputField(fieldName);
    f.setValue(values, 1.0F);
    return f;
  }

}
