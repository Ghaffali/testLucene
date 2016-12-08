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

package org.apache.solr.update;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest.Field;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse.FieldResponse;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.RefCounted;
import org.apache.zookeeper.KeeperException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the in-place updates (docValues updates) for a one shard, three replica cluster.
 */
@Slow
public class TestInPlaceUpdatesDistrib extends AbstractFullDistribZkTestBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeSuperClass() throws Exception {

    // nocommit: does this test need to randomize between diff schema/fields used?
    // nocommit: see nocommits/jira questions related to special dynamicField logic in AtomicUpdateDocumentMerger.isInPlaceUpdate
    
    schemaString = "schema-inplace-updates.xml";
    configString = "solrconfig-tlog.xml";

    // sanity check that autocommits are disabled
    initCore(configString, schemaString);
    assertEquals(-1, h.getCore().getSolrConfig().getUpdateHandlerInfo().autoCommmitMaxTime);
    assertEquals(-1, h.getCore().getSolrConfig().getUpdateHandlerInfo().autoSoftCommmitMaxTime);
    assertEquals(-1, h.getCore().getSolrConfig().getUpdateHandlerInfo().autoCommmitMaxDocs);
    assertEquals(-1, h.getCore().getSolrConfig().getUpdateHandlerInfo().autoSoftCommmitMaxDocs);
  }

  public TestInPlaceUpdatesDistrib() throws Exception {
    super();
    sliceCount = 1;
    fixShardCount(3);
  }

  private SolrClient LEADER = null;
  private List<SolrClient> NONLEADERS = null;
  
  @Test
  @ShardsFixed(num = 3)
  @SuppressWarnings("unchecked")
  public void test() throws Exception {
    waitForRecoveriesToFinish(true);
    mapReplicasToClients();
    
    // sanity check no one broke the assumptions we make about our schema
    checkExpectedSchemaField(map("name", "inplace_updatable_int",
        "type","int",
        "stored",Boolean.FALSE,
        "indexed",Boolean.FALSE,
        "docValues",Boolean.TRUE,
        "default","0"));
    checkExpectedSchemaField(map("name", "inplace_updatable_float",
        "type","float",
        "stored",Boolean.FALSE,
        "indexed",Boolean.FALSE,
        "docValues",Boolean.TRUE,
        "default","0"));
    checkExpectedSchemaField(map("name", "_version_",
        "type","long",
        "stored",Boolean.FALSE,
        "indexed",Boolean.FALSE,
        "docValues",Boolean.TRUE));

    // Do the tests now:
    outOfOrderDBQsTest();
    docValuesUpdateTest();
    ensureRtgWorksWithPartialUpdatesTest();
    delayedReorderingFetchesMissingUpdateFromLeaderTest();
    outOfOrderUpdatesIndividualReplicaTest();
    outOfOrderDeleteUpdatesIndividualReplicaTest();
    reorderedDBQsWithInPlaceUpdatesShouldNotThrowReplicaInLIRTest();
  }
  
  private void mapReplicasToClients() throws KeeperException, InterruptedException {
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    cloudClient.getZkStateReader().forceUpdateCollection(DEFAULT_COLLECTION);
    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    Replica leader = null;
    Slice shard1 = clusterState.getCollection(DEFAULT_COLLECTION).getSlice(SHARD1);
    leader = shard1.getLeader();

    String leaderBaseUrl = zkStateReader.getBaseUrlForNodeName(leader.getNodeName());
    for (int i=0; i<clients.size(); i++) {
      if (((HttpSolrClient)clients.get(i)).getBaseURL().startsWith(leaderBaseUrl))
        LEADER = clients.get(i);
    }
    
    NONLEADERS = new ArrayList<>();
    for (Replica rep: shard1.getReplicas()) {
      if (rep.equals(leader)) {
        continue;
      }
      String baseUrl = zkStateReader.getBaseUrlForNodeName(rep.getNodeName());
      for (int i=0; i<clients.size(); i++) {
        if (((HttpSolrClient)clients.get(i)).getBaseURL().startsWith(baseUrl))
          NONLEADERS.add(clients.get(i));
      }
    }
    
    assertNotNull(LEADER);
    assertEquals(2, NONLEADERS.size());
  }

  final int NUM_RETRIES = 100, WAIT_TIME = 10;

  // The following should work: full update to doc 0, in-place update for doc 0, delete doc 0
  private void outOfOrderDBQsTest() throws Exception {
    
    del("*:*");
    commit();
    
    buildRandomIndex(0);

    float inplace_updatable_float = 1;

    // update doc, set
    index("id", 0, "inplace_updatable_float", map("set", inplace_updatable_float));

    LEADER.commit();
    SolrDocument sdoc = LEADER.getById("0");  // RTG straight from the index
    assertEquals(inplace_updatable_float, sdoc.get("inplace_updatable_float"));
    assertEquals("title0", sdoc.get("title_s"));
    long version0 = (long) sdoc.get("_version_");

    // put replica out of sync
    float newinplace_updatable_float = 100;
    List<UpdateRequest> updates = new ArrayList<>();
    updates.add(simulatedUpdateRequest(null, "id", 0, "title_s", "title0_new", "inplace_updatable_float", newinplace_updatable_float, "_version_", version0 + 1)); // full update
    updates.add(simulatedUpdateRequest(version0 + 1, "id", 0, "inplace_updatable_float", newinplace_updatable_float + 1, "_version_", version0 + 2)); // inplace_updatable_float=101
    updates.add(simulatedDeleteRequest(0, version0 + 3));

    // order the updates correctly for NONLEADER 1
    for (UpdateRequest update : updates) {
      log.info("Issuing well ordered update: " + update.getDocuments());
      NONLEADERS.get(1).request(update);
    }

    // Reordering needs to happen using parallel threads
    ExecutorService threadpool = 
        ExecutorUtil.newMDCAwareFixedThreadPool(updates.size() + 1, new DefaultSolrThreadFactory(getTestName()));

    // re-order the updates for NONLEADER 0
    List<UpdateRequest> reorderedUpdates = new ArrayList<>(updates);
    Collections.shuffle(reorderedUpdates, random());
    List<Future<UpdateResponse>> updateResponses = new ArrayList<>();
    for (UpdateRequest update : reorderedUpdates) {
      AsyncUpdateWithRandomCommit task = new AsyncUpdateWithRandomCommit(update, NONLEADERS.get(0), random().nextLong());
      updateResponses.add(threadpool.submit(task));
      // while we can't guarantee/trust what order the updates are executed in, since multiple threads
      // are involved, but we're trying to bias the thread scheduling to run them in the order submitted
      Thread.sleep(10);
    }
    
    threadpool.shutdown();
    assertTrue("Thread pool didn't terminate within 10 secs", threadpool.awaitTermination(10, TimeUnit.SECONDS));
    
    // assert all requests were successful
    for (Future<UpdateResponse> resp: updateResponses) {
      assertEquals(0, resp.get().getStatus());
    }

    // assert both replicas have same effect
    for (SolrClient client : NONLEADERS) { // 0th is re-ordered replica, 1st is well-ordered replica
      SolrDocument doc = client.getById(String.valueOf(0), params("distrib", "false"));
      assertNull("This doc was supposed to have been deleted, but was: " + doc, doc);
    }

    log.info("outOfOrderDeleteUpdatesIndividualReplicaTest: This test passed fine...");
    del("*:*");
    commit();
  }

  private void docValuesUpdateTest() throws Exception {
    del("*:*");
    commit();

    int numDocs = atLeast(100);
    log.info("Trying num docs = " + numDocs);

    // nocommit: reuse buildRandomIndex here (ideally with randomized initFloat)
    // nocommit: switching to it isn't trivial, because many places in test assume no docs in index other then ones being udpated
    //
    // nocommit: doing that and changing rest of method to only look at specialIds (positive ids)
    // nocommit: to ensure we don't hit have any edge case bugs where this test only passes because
    // nocommit: all docs are updated (or all in a segment, etc...)
    for (int i = 0; i < numDocs; i++) {
      index("id", i, "title_s", "title" + i, "id_i", i, "inplace_updatable_float", "101.0");
    }
    commit();
    
    List<Integer> luceneDocids = new ArrayList<>();
    ModifiableSolrParams params = params("q", "*:*", "fl", "*,[docid]", "rows", String.valueOf(numDocs), "sort", "id_i asc");
    SolrDocumentList results = LEADER.query(params).getResults();
    assertEquals(numDocs, results.size());
    for (SolrDocument doc : results) {
      luceneDocids.add((int) doc.get("[docid]"));
    }
    log.info("Initial results: "+results);
    List<Float> valuesList = new ArrayList<Float>();
    for (int i = 0; i < numDocs; i++) {
      valuesList.add(r.nextFloat()*5.0f);
    }
    log.info("inplace_updatable_float: "+valuesList);
    // update doc w/ set
    for (int i = numDocs - 1; i >= 0; i--) {
      index("id", i, "inplace_updatable_float", map("set", valuesList.get(i)));
    }

    commit();

    // Keep querying until the commit is distributed and expected number of results are obtained
    boolean expectedResults = false;
    long numFound = 0;
    // nocommit: need to loop to check all (non-leader) clients -- see jira for elabroration
    SolrClient clientToTest = clients.get(random().nextInt(clients.size()));
    for (int retries=0; retries < NUM_RETRIES; retries++) {
      log.info("Attempt: "+retries+", Results: "+results);

      // nocommit: useless validation query since every doc starts off with a value when index is built
      // nocommit: see jira comment for more detailed suggestion on fix
      //
      // nocommit: inplace_updatable_float has a schema default="0" -- which means even if we switch to using buildRandomIndex, this check is useless
      numFound = clientToTest.query(
          params("q", "inplace_updatable_float:[* TO *]")).getResults().getNumFound();
      if (numFound != numDocs) {
        Thread.sleep(WAIT_TIME);
      } else {
        expectedResults = true;
        break;
      }
    }
    if (!expectedResults) {
      fail("Waited " + (NUM_RETRIES*WAIT_TIME/1000) + " secs, but not obtained expected results. numFound: "
          + numFound + ", numDocs: " + numDocs);
    }

    // nocommit: picking random from "clients" means LEADER is included, making tests uselss 1/3 the time
    // nocommit: need to loop to check all (non-leader) clients -- see jira for elabroration
    results = clients.get(random().nextInt(clients.size())).query(params).getResults();
    assertTrue(matchResults(results, luceneDocids, valuesList));
    
    // update doc, increment
    // ensure all ratings have a value such that abs(ratings) < X, 
    // then the second update can use an increment such that abs(inc) > X*3
    // and we can use -ratings:[-X TO X] as the query in a retry loop
    log.info("Updating the documents...");
    // nocommit: what if min(valuesList) and max(valuesList) are both negative?
    // nocommit: don't we need the abs of both values?
    // nocommit: see jira comment for longer discussion about eliminating need to find X dynamically
    float X = Math.max(Collections.max(valuesList), Math.abs(Collections.min(valuesList)));
    for (int i = 0; i < numDocs; i++) {
      int inc = r.nextBoolean()? (int)(Math.ceil(X*3)) + r.nextInt(1000): (int)(-Math.ceil(X*3)) - r.nextInt(1000);
      valuesList.set(i, valuesList.get(i) + inc);
      index("id", i, "inplace_updatable_float", map("inc", inc));
    }
    commit();
    
    // Keep querying until the commit is distributed and expected results are obtained
    expectedResults = false;
    for (int retries=0; retries < NUM_RETRIES; retries++) {
      log.info("Attempt: "+retries+", Results: "+results);
      numFound = clientToTest.query(
          params("q", "+inplace_updatable_float:[* TO *] -inplace_updatable_float:[-"+X+" TO "+X+"]"))
          .getResults().getNumFound();
      if (numFound != numDocs) {
        Thread.sleep(WAIT_TIME);
      } else {
        expectedResults = true;
        break;
      }    
    }
    if (!expectedResults) {
      fail("Waited " + (NUM_RETRIES*WAIT_TIME/1000) + " secs, but not obtained expected results. numFound: "
          + numFound + ", numDocs: " + numDocs);
    }
    // nocommit: picking random from "clients" means LEADER is included, making tests uselss 1/3 the time
    // nocommit: need to loop to check all (non-leader) clients -- see jira for elabroration
    results = clients.get(random().nextInt(clients.size())).query(params).getResults();
    assertTrue(matchResults(results, luceneDocids, valuesList));
  }

  /**
   * Return true if the (same ordered) luceneDocids & ratings match the results
   */
  private boolean matchResults(SolrDocumentList results, List<Integer> luceneDocids, List<Float> valuesList) {
    // nocommit: method should just assert expected values, not return boolean
    // nocommit: simplifies caller code, and protects against risk of missuse in future (tests forgeting to check return value)
    // nocommit: should rename something like "assertDocIdsAndValuesInResults"
    
    int counter = 0;
    if (luceneDocids.size() == results.size()) {
      for (SolrDocument doc : results) {
        float r = (float) doc.get("inplace_updatable_float");
        int id = (int) doc.get("[docid]");

        if (!(luceneDocids.get(counter).intValue() == id &&
            Math.abs(valuesList.get(counter).floatValue() - r) <= 0.001)) {
          return false;
        }
        counter++;
      }
    } else {
      return false;
    }
    
    return true;
  }
  
  
  private void ensureRtgWorksWithPartialUpdatesTest() throws Exception {
    del("*:*");
    commit();

    float inplace_updatable_float = 1;
    String title = "title100";
    long version = 0, currentVersion;

    currentVersion = buildRandomIndex(100).get(0);
    assertTrue(currentVersion > version);
    
    // get the internal docids of id=100 document from the three replicas
    List<Integer> docids = getInternalDocIds("100");

    // update doc, set
    currentVersion = addDocAndGetVersion("id", 100, "inplace_updatable_float", map("set", inplace_updatable_float));
    assertTrue(currentVersion > version);
    version = currentVersion;
    LEADER.commit();
    assertTrue("Earlier: "+docids+", now: "+getInternalDocIds("100"), docids.equals(getInternalDocIds("100")));
    
    SolrDocument sdoc = LEADER.getById("100");  // RTG straight from the index
    assertEquals(sdoc.toString(), (float) inplace_updatable_float, sdoc.get("inplace_updatable_float"));
    assertEquals(sdoc.toString(), "title100", sdoc.get("title_s"));
    assertEquals(sdoc.toString(), version, sdoc.get("_version_"));

    if(random().nextBoolean()) {
      title = "newtitle100";
      currentVersion = addDocAndGetVersion("id", 100, "title_s", title, "inplace_updatable_float", inplace_updatable_float); // full indexing
      assertTrue(currentVersion > version);
      version = currentVersion;
      docids = getInternalDocIds("100");
    }

    inplace_updatable_float++;
    currentVersion = addDocAndGetVersion("id", 100, "inplace_updatable_float", map("inc", 1));
    assertTrue(currentVersion > version);
    version = currentVersion;
    LEADER.commit();
    assertTrue("Earlier: "+docids+", now: "+getInternalDocIds("100"), docids.equals(getInternalDocIds("100")));
    
    currentVersion = addDocAndGetVersion("id", 100, "inplace_updatable_int", map("set", "100"));
    assertTrue(currentVersion > version);
    version = currentVersion;

    inplace_updatable_float++;
    currentVersion = addDocAndGetVersion("id", 100, "inplace_updatable_float", map("inc", 1));
    assertTrue(currentVersion > version);
    version = currentVersion;

    // RTG from tlog
    // nocommit: picking random from "clients" means LEADER is included, making tests uselss 1/3 the time
    sdoc = clients.get(random().nextInt(clients.size())).getById("100");

    assertEquals(sdoc.toString(), (int) 100, sdoc.get("inplace_updatable_int"));
    assertEquals(sdoc.toString(), (float) inplace_updatable_float, sdoc.get("inplace_updatable_float"));
    assertEquals(sdoc.toString(), title, sdoc.get("title_s"));
    assertEquals(sdoc.toString(), version, sdoc.get("_version_"));
    
    // assert that the internal docid for id=100 document remains same, in each replica, as before
    LEADER.commit();
    assertTrue("Earlier: "+docids+", now: "+getInternalDocIds("100"), docids.equals(getInternalDocIds("100")));
  }
  
  private List<Integer> getInternalDocIds(String id) throws IOException {
    // nocommit: no reason for ths method to use SolrIndexSearcher directly -- will make converting to cloud test later hard
    // nocommit: should just use RTG w/fl=[docid] to check values
    // nocommit: seems like method created because of SOLR-9289, but that's been fixed for a while.
    
    List<Integer> ret = new ArrayList<>();
    for (CloudJettyRunner jetty: shardToJetty.get(SHARD1)) {
      try (SolrCore core = jetty.jetty.getCoreContainer().getCore(DEFAULT_TEST_COLLECTION_NAME)) {
        RefCounted<SolrIndexSearcher> holder = core.openNewSearcher(false, true);
        try {
          SolrIndexSearcher searcher = holder.get();
          int docId = searcher.search(new TermQuery(new Term("id", id)), 1).scoreDocs[0].doc;
          ret.add(docId);
          // debug: System.out.println("gIDI: "+searcher.doc(docId));
        } finally {
          holder.decref();
        }
      }
    }
    return ret;
  }

  private void outOfOrderUpdatesIndividualReplicaTest() throws Exception {
    
    del("*:*");
    commit();

    buildRandomIndex(0);

    float inplace_updatable_float = 1;
    // update doc, set
    index("id", 0, "inplace_updatable_float", map("set", inplace_updatable_float));

    LEADER.commit();
    SolrDocument sdoc = LEADER.getById("0");  // RTG straight from the index
    assertEquals(inplace_updatable_float, sdoc.get("inplace_updatable_float"));
    assertEquals("title0", sdoc.get("title_s"));
    long version0 = (long) sdoc.get("_version_");

    // put replica out of sync
    float newinplace_updatable_float = 100;
    List<UpdateRequest> updates = new ArrayList<>();
    updates.add(simulatedUpdateRequest(null, "id", 0, "title_s", "title0_new", "inplace_updatable_float", newinplace_updatable_float, "_version_", version0 + 1)); // full update
    for (int i=1; i<atLeast(3); i++) {
      updates.add(simulatedUpdateRequest(version0 + i, "id", 0, "inplace_updatable_float", newinplace_updatable_float + i, "_version_", version0 + i + 1));
    }

    // order the updates correctly for NONLEADER 1
    for (UpdateRequest update : updates) {
      log.info("Issuing well ordered update: " + update.getDocuments());
      NONLEADERS.get(1).request(update);
    }

    // Reordering needs to happen using parallel threads, since some of these updates will
    // be blocking calls, waiting for some previous updates to arrive on which it depends.
    ExecutorService threadpool = 
        ExecutorUtil.newMDCAwareFixedThreadPool(updates.size() + 1, new DefaultSolrThreadFactory(getTestName()));

    // re-order the updates for NONLEADER 0
    List<UpdateRequest> reorderedUpdates = new ArrayList<>(updates);
    Collections.shuffle(reorderedUpdates, random());
    List<Future<UpdateResponse>> updateResponses = new ArrayList<>();
    for (UpdateRequest update : reorderedUpdates) {
      AsyncUpdateWithRandomCommit task = new AsyncUpdateWithRandomCommit(update, NONLEADERS.get(0), random().nextLong());
      updateResponses.add(threadpool.submit(task));
      // while we can't guarantee/trust what order the updates are executed in, since multiple threads
      // are involved, but we're trying to bias the thread scheduling to run them in the order submitted
      Thread.sleep(10);
    }
    
    threadpool.shutdown();
    assertTrue("Thread pool didn't terminate within 10 secs", threadpool.awaitTermination(10, TimeUnit.SECONDS));

    // assert all requests were successful
    for (Future<UpdateResponse> resp: updateResponses) {
      assertEquals(0, resp.get().getStatus());
    }

    // assert both replicas have same effect
    for (SolrClient client : NONLEADERS) { // 0th is re-ordered replica, 1st is well-ordered replica
      log.info("Testing client: " + ((HttpSolrClient)client).getBaseURL());
      assertReplicaValue(client, 0, "inplace_updatable_float", (newinplace_updatable_float + (float)(updates.size() - 1)), 
          "inplace_updatable_float didn't match for replica at client: " + ((HttpSolrClient)client).getBaseURL());
      assertReplicaValue(client, 0, "title_s", "title0_new", 
          "Title didn't match for replica at client: " + ((HttpSolrClient)client).getBaseURL());
      assertEquals(version0 + updates.size(), getReplicaValue(client, 0, "_version_"));
    }

    log.info("outOfOrderUpdatesIndividualReplicaTest: This test passed fine...");
    del("*:*");
    commit();
  }
  
  // The following should work: full update to doc 0, in-place update for doc 0, delete doc 0
  private void outOfOrderDeleteUpdatesIndividualReplicaTest() throws Exception {
    
    del("*:*");
    commit();

    buildRandomIndex(0);

    float inplace_updatable_float = 1;
    // update doc, set
    index("id", 0, "inplace_updatable_float", map("set", inplace_updatable_float));

    LEADER.commit();
    SolrDocument sdoc = LEADER.getById("0");  // RTG straight from the index
    assertEquals(inplace_updatable_float, sdoc.get("inplace_updatable_float"));
    assertEquals("title0", sdoc.get("title_s"));
    long version0 = (long) sdoc.get("_version_");

    // put replica out of sync
    float newinplace_updatable_float = 100;
    List<UpdateRequest> updates = new ArrayList<>();
    updates.add(simulatedUpdateRequest(null, "id", 0, "title_s", "title0_new", "inplace_updatable_float", newinplace_updatable_float, "_version_", version0 + 1)); // full update
    updates.add(simulatedUpdateRequest(version0 + 1, "id", 0, "inplace_updatable_float", newinplace_updatable_float + 1, "_version_", version0 + 2)); // inplace_updatable_float=101
    updates.add(simulatedDeleteRequest(0, version0 + 3));

    // order the updates correctly for NONLEADER 1
    for (UpdateRequest update : updates) {
      log.info("Issuing well ordered update: " + update.getDocuments());
      NONLEADERS.get(1).request(update);
    }

    // Reordering needs to happen using parallel threads
    ExecutorService threadpool = 
        ExecutorUtil.newMDCAwareFixedThreadPool(updates.size() + 1, new DefaultSolrThreadFactory(getTestName()));

    // re-order the updates for NONLEADER 0
    List<UpdateRequest> reorderedUpdates = new ArrayList<>(updates);
    Collections.shuffle(reorderedUpdates, random());
    List<Future<UpdateResponse>> updateResponses = new ArrayList<>();
    for (UpdateRequest update : reorderedUpdates) {
      AsyncUpdateWithRandomCommit task = new AsyncUpdateWithRandomCommit(update, NONLEADERS.get(0), random().nextLong());
      updateResponses.add(threadpool.submit(task));
      // while we can't guarantee/trust what order the updates are executed in, since multiple threads
      // are involved, but we're trying to bias the thread scheduling to run them in the order submitted
      Thread.sleep(10);
    }
    
    threadpool.shutdown();
    assertTrue("Thread pool didn't terminate within 10 secs", threadpool.awaitTermination(10, TimeUnit.SECONDS));

    // assert all requests were successful
    for (Future<UpdateResponse> resp: updateResponses) {
      assertEquals(0, resp.get().getStatus());
    }

    // assert both replicas have same effect
    for (SolrClient client : NONLEADERS) { // 0th is re-ordered replica, 1st is well-ordered replica
      SolrDocument doc = client.getById(String.valueOf(0), params("distrib", "false"));
      assertNull("This doc was supposed to have been deleted, but was: " + doc, doc);
    }

    log.info("outOfOrderDeleteUpdatesIndividualReplicaTest: This test passed fine...");
    del("*:*");
    commit();
  }

  /* Test for a situation when a document requiring in-place update cannot be "resurrected"
   * when the original full indexed document has been deleted by an out of order DBQ.
   * Expected behaviour in this case should be to throw the replica into LIR (since this will
   * be rare). Here's an example of the situation:
        ADD(id=x, val=5, ver=1)
        UPD(id=x, val=10, ver = 2)
        DBQ(q=val:10, v=4)
        DV(id=x, val=5, ver=3)
   */
  private void reorderedDBQsWithInPlaceUpdatesShouldNotThrowReplicaInLIRTest() throws Exception {
    del("*:*");
    commit();

    buildRandomIndex(0);

    SolrDocument sdoc = LEADER.getById("0");  // RTG straight from the index
    //assertEquals(value, sdoc.get("inplace_updatable_float"));
    assertEquals("title0", sdoc.get("title_s"));
    long version0 = (long) sdoc.get("_version_");

    String field = "inplace_updatable_int";
    
    // put replica out of sync
    List<UpdateRequest> updates = new ArrayList<>();
    updates.add(simulatedUpdateRequest(null, "id", 0, "title_s", "title0_new", field, 5, "_version_", version0 + 1)); // full update
    updates.add(simulatedUpdateRequest(version0 + 1, "id", 0, field, 10, "_version_", version0 + 2)); // inplace_updatable_float=101
    updates.add(simulatedUpdateRequest(version0 + 2, "id", 0, field, 5, "_version_", version0 + 3)); // inplace_updatable_float=101
    updates.add(simulatedDeleteRequest(field+":10", version0 + 4)); // supposed to not delete anything

    // order the updates correctly for NONLEADER 1
    for (UpdateRequest update : updates) {
      log.info("Issuing well ordered update: " + update.getDocuments());
      NONLEADERS.get(1).request(update);
    }

    // Reordering needs to happen using parallel threads
    ExecutorService threadpool = 
        ExecutorUtil.newMDCAwareFixedThreadPool(updates.size() + 1, new DefaultSolrThreadFactory(getTestName()));
    long seed = random().nextLong(); // seed for randomization within the threads

    // re-order the last two updates for NONLEADER 0
    List<UpdateRequest> reorderedUpdates = new ArrayList<>(updates);
    Collections.swap(reorderedUpdates, 2, 3);
    
    List<Future<UpdateResponse>> updateResponses = new ArrayList<>();
    for (UpdateRequest update : reorderedUpdates) {
      // pretend as this update is coming from the other non-leader, so that
      // the resurrection can happen from there (instead of the leader)
      update.setParam(DistributedUpdateProcessor.DISTRIB_FROM, ((HttpSolrClient)NONLEADERS.get(1)).getBaseURL());
      AsyncUpdateWithRandomCommit task = new AsyncUpdateWithRandomCommit(update, NONLEADERS.get(0), seed);
      updateResponses.add(threadpool.submit(task));
      // while we can't guarantee/trust what order the updates are executed in, since multiple threads
      // are involved, but we're trying to bias the thread scheduling to run them in the order submitted
      Thread.sleep(10);
    }
    
    threadpool.shutdown();
    assertTrue("Thread pool didn't terminate within 10 secs", threadpool.awaitTermination(10, TimeUnit.SECONDS));

    int successful = 0;
    for (Future<UpdateResponse> resp: updateResponses) {
      try {
        UpdateResponse r = resp.get();
        if (r.getStatus() == 0) {
          successful++;
        }
      } catch (Exception ex) {
        // reordered DBQ should trigger an error, thus throwing the replica into LIR.
        // the cause of the error is that the full document was deleted by mistake due to the
        // out of order DBQ, and the in-place update that arrives after the DBQ (but was supposed to 
        // arrive before) cannot be applied, since the full document can't now be "resurrected".

        if (!ex.getMessage().contains("Tried to fetch missing update"
            + " from the leader, but missing wasn't present at leader.")) {
          throw ex;
        }
      }
    }
    // All should succeed, i.e. no LIR
    assertEquals(updateResponses.size(), successful);
    
    log.info("Non leader 0: "+((HttpSolrClient)NONLEADERS.get(0)).getBaseURL());
    log.info("Non leader 1: "+((HttpSolrClient)NONLEADERS.get(1)).getBaseURL());
    
    SolrDocument doc0 = NONLEADERS.get(0).getById(String.valueOf(0), params("distrib", "false"));
    SolrDocument doc1 = NONLEADERS.get(1).getById(String.valueOf(0), params("distrib", "false"));

    log.info("Doc in both replica 0: "+doc0);
    log.info("Doc in both replica 1: "+doc1);
    // assert both replicas have same effect
    for (int i=0; i<NONLEADERS.size(); i++) { // 0th is re-ordered replica, 1st is well-ordered replica
      SolrClient client = NONLEADERS.get(i);
      SolrDocument doc = client.getById(String.valueOf(0), params("distrib", "false"));
      assertNotNull("Client: "+((HttpSolrClient)client).getBaseURL(), doc);
      assertEquals("Client: "+((HttpSolrClient)client).getBaseURL(), 5, doc.getFieldValue(field));
    }

    log.info("reorderedDBQsWithInPlaceUpdatesShouldNotThrowReplicaInLIRTest: This test passed fine...");
    del("*:*");
    commit();
  }
  
  private void delayedReorderingFetchesMissingUpdateFromLeaderTest() throws Exception {
    del("*:*");
    commit();
    
    float inplace_updatable_float = 1F;
    buildRandomIndex(inplace_updatable_float, false, 1);

    float newinplace_updatable_float = 100F;
    List<UpdateRequest> updates = new ArrayList<>();
    updates.add(regularUpdateRequest("id", 1, "title_s", "title1_new", "id_i", 1, "inplace_updatable_float", newinplace_updatable_float));
    updates.add(regularUpdateRequest("id", 1, "inplace_updatable_float", map("inc", 1)));
    updates.add(regularUpdateRequest("id", 1, "inplace_updatable_float", map("inc", 1)));

    // The next request to replica2 will be delayed by 6 secs (timeout is 5s)
    shardToJetty.get(SHARD1).get(1).jetty.getDebugFilter().addDelay(
        "Waiting for dependant update to timeout", 1, 6000);

    long seed = random().nextLong(); // seed for randomization within the threads
    ExecutorService threadpool =
        ExecutorUtil.newMDCAwareFixedThreadPool(updates.size() + 1, new DefaultSolrThreadFactory(getTestName()));
    for (UpdateRequest update : updates) {
      AsyncUpdateWithRandomCommit task = new AsyncUpdateWithRandomCommit(update, cloudClient, seed);
      threadpool.submit(task);

      // while we can't guarantee/trust what order the updates are executed in, since multiple threads
      // are involved, but we're trying to bias the thread scheduling to run them in the order submitted
      Thread.sleep(100); 
    }

    threadpool.shutdown();
    assertTrue("Thread pool didn't terminate within 10 secs", threadpool.awaitTermination(10, TimeUnit.SECONDS));

    commit();

    // TODO: Could try checking ZK for LIR flags to ensure LIR has not kicked in
    // Check every 10ms, 100 times, for a replica to go down (& assert that it doesn't)
    for (int i=0; i<100; i++) {
      Thread.sleep(10);
      cloudClient.getZkStateReader().forceUpdateCollection(DEFAULT_COLLECTION);
      ClusterState state = cloudClient.getZkStateReader().getClusterState();

      int numActiveReplicas = 0;
      for (Replica rep: state.getCollection(DEFAULT_COLLECTION).getSlice(SHARD1).getReplicas())
        if (rep.getState().equals(Replica.State.ACTIVE))
          numActiveReplicas++;

      assertEquals("The replica receiving reordered updates must not have gone down", 3, numActiveReplicas);
    }

    // nocommit: what's the point of this ad-hoc array?
    // nocommit: if we want the leader and all non leaders, why not just loop over "clients" ?
    for (SolrClient client: new SolrClient[] {LEADER, NONLEADERS.get(0), 
        NONLEADERS.get(1)}) { // nonleader 0 re-ordered replica, nonleader 1 well-ordered replica
      log.info("Testing client (Fetch missing test): " + ((HttpSolrClient)client).getBaseURL());
      log.info("Version at " + ((HttpSolrClient)client).getBaseURL() + " is: " + getReplicaValue(client, 1, "_version_"));

      assertReplicaValue(client, 1, "inplace_updatable_float", (newinplace_updatable_float + 2.0f), 
          "inplace_updatable_float didn't match for replica at client: " + ((HttpSolrClient)client).getBaseURL());
      assertReplicaValue(client, 1, "title_s", "title1_new", 
          "Title didn't match for replica at client: " + ((HttpSolrClient)client).getBaseURL());
    }
    
    // Try another round of these updates, this time with a delete request at the end.
    // This is to ensure that the fetch missing update from leader doesn't bomb out if the 
    // document has been deleted on the leader later on
    {
      del("*:*");
      commit();
      shardToJetty.get(SHARD1).get(1).jetty.getDebugFilter().unsetDelay();
      
      updates.add(regularDeleteRequest(1));

      shardToJetty.get(SHARD1).get(1).jetty.getDebugFilter().addDelay("Waiting for dependant update to timeout", 1, 5999); // the first update
      shardToJetty.get(SHARD1).get(1).jetty.getDebugFilter().addDelay("Waiting for dependant update to timeout", 4, 5998); // the delete update

      threadpool =
          ExecutorUtil.newMDCAwareFixedThreadPool(updates.size() + 1, new DefaultSolrThreadFactory(getTestName()));
      for (UpdateRequest update : updates) {
        AsyncUpdateWithRandomCommit task = new AsyncUpdateWithRandomCommit(update, cloudClient, seed);
        threadpool.submit(task);
        
        // while we can't guarantee/trust what order the updates are executed in, since multiple threads
        // are involved, but we're trying to bias the thread scheduling to run them in the order submitted
        Thread.sleep(100);
      }

      threadpool.shutdown();
      assertTrue("Thread pool didn't terminate within 15 secs", threadpool.awaitTermination(15, TimeUnit.SECONDS));

      commit();

      // TODO: Could try checking ZK for LIR flags to ensure LIR has not kicked in
      // Check every 10ms, 100 times, for a replica to go down (& assert that it doesn't)
      ZkController zkController = shardToLeaderJetty.get(SHARD1).jetty.getCoreContainer().getZkController();
      String lirPath = zkController.getLeaderInitiatedRecoveryZnodePath(DEFAULT_TEST_COLLECTION_NAME, SHARD1);
      assertFalse (zkController.getZkClient().exists(lirPath, true));

      for (int i=0; i<100; i++) {
        Thread.sleep(10);
        cloudClient.getZkStateReader().forceUpdateCollection(DEFAULT_COLLECTION);
        ClusterState state = cloudClient.getZkStateReader().getClusterState();

        int numActiveReplicas = 0;
        for (Replica rep: state.getCollection(DEFAULT_COLLECTION).getSlice(SHARD1).getReplicas())
          if (rep.getState().equals(Replica.State.ACTIVE))
            numActiveReplicas++;

        assertEquals("The replica receiving reordered updates must not have gone down", 3, numActiveReplicas);
      }

      for (SolrClient client: new SolrClient[] {LEADER, NONLEADERS.get(0), 
          NONLEADERS.get(1)}) { // nonleader 0 re-ordered replica, nonleader 1 well-ordered replica
        SolrDocument doc = client.getById(String.valueOf(1), params("distrib", "false"));
        assertNull("This doc was supposed to have been deleted, but was: " + doc, doc);
      }

    }
    log.info("delayedReorderingFetchesMissingUpdateFromLeaderTest: This test passed fine...");
  }

  /**
   * Use the schema API to verify that the specified expected Field exists with those exact attributes. 
   */
  public void checkExpectedSchemaField(Map<String,Object> expected) throws Exception {
    String fieldName = (String) expected.get("name");
    assertNotNull("expected contains no name: " + expected, fieldName);
    FieldResponse rsp = new Field(fieldName).process(this.cloudClient);
    assertNotNull("Field Null Response: " + fieldName, rsp);
    assertEquals("Field Status: " + fieldName + " => " + rsp.toString(), 0, rsp.getStatus());
    assertEquals("Field: " + fieldName, expected, rsp.getField());
  }

  private static class AsyncUpdateWithRandomCommit implements Callable<UpdateResponse> {
    UpdateRequest update;
    SolrClient solrClient;
    final Random rnd;

    public AsyncUpdateWithRandomCommit (UpdateRequest update, SolrClient solrClient, long seed) {
      this.update = update;
      this.solrClient = solrClient;
      this.rnd = new Random(seed);
    }

    @Override
    public UpdateResponse call() throws Exception {
      UpdateResponse resp = update.process(solrClient); //solrClient.request(update);
      if (rnd.nextInt(3) == 0)
        solrClient.commit();
      return resp;
    }
  }
  
  Object getReplicaValue(SolrClient client, int doc, String field) throws SolrServerException, IOException {
    // nocommit: all other usages of SolrClient.getById in this class should be replaced by calls to this method
    // nocommit: that way we ensure distrib=false
    SolrDocument sdoc = client.getById(String.valueOf(doc), params("distrib", "false"));
    return sdoc.get(field);
  }

  void assertReplicaValue(SolrClient client, int doc, String field, Object expected,
      String message) throws SolrServerException, IOException {
    assertEquals(message, expected, getReplicaValue(client, doc, field));
  }

  // This returns an UpdateRequest with the given fields that represent a document.
  // This request is constructed such that it is a simulation of a request coming from
  // a leader to a replica.
  UpdateRequest simulatedUpdateRequest(Long prevVersion, Object... fields) throws SolrServerException, IOException {
    SolrInputDocument doc = sdoc(fields);
    
    // get baseUrl of the leader
    String baseUrl = getBaseUrl(doc.get("id").toString());

    UpdateRequest ur = new UpdateRequest();
    ur.add(doc);
    ur.setParam("update.distrib", "FROMLEADER");
    if (prevVersion != null) {
      ur.setParam("distrib.inplace.prevversion", String.valueOf(prevVersion));
      ur.setParam("distrib.inplace.update", "true");
    }
    ur.setParam("distrib.from", baseUrl);
    return ur;
  }

  UpdateRequest simulatedDeleteRequest(int id, long version) throws SolrServerException, IOException {
    String baseUrl = getBaseUrl(""+id);

    UpdateRequest ur = new UpdateRequest();
    if (random().nextBoolean()) {
      ur.deleteById(""+id);
    } else {
      ur.deleteByQuery("id:"+id);
    }
    ur.setParam("_version_", ""+version);
    ur.setParam("update.distrib", "FROMLEADER");
    ur.setParam("distrib.from", baseUrl);
    return ur;
  }

  UpdateRequest simulatedDeleteRequest(String query, long version) throws SolrServerException, IOException {
    // nocommit: why clients.get(0)? ... if we want LEADER why not just use LEADER ?
    String baseUrl = getBaseUrl((HttpSolrClient)clients.get(0));

    UpdateRequest ur = new UpdateRequest();
    ur.deleteByQuery(query);
    ur.setParam("_version_", ""+version);
    ur.setParam("update.distrib", "FROMLEADER");
    ur.setParam("distrib.from", baseUrl + DEFAULT_COLLECTION + "/");
    return ur;
  }

  private String getBaseUrl(String id) {
    DocCollection collection = cloudClient.getZkStateReader().getClusterState().getCollection(DEFAULT_COLLECTION);
    Slice slice = collection.getRouter().getTargetSlice(id, null, null, null, collection);
    String baseUrl = slice.getLeader().getCoreUrl();
    return baseUrl;
  }

  UpdateRequest regularUpdateRequest(Object... fields) throws SolrServerException, IOException {
    UpdateRequest ur = new UpdateRequest();
    SolrInputDocument doc = sdoc(fields);
    ur.add(doc);
    return ur;
  }

  UpdateRequest regularDeleteRequest(int id) throws SolrServerException, IOException {
    UpdateRequest ur = new UpdateRequest();
    ur.deleteById(""+id);
    return ur;
  }

  @SuppressWarnings("rawtypes")
  protected long addDocAndGetVersion(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("versions", "true");
    
    UpdateRequest ureq = new UpdateRequest();
    ureq.setParams(params);
    ureq.add(doc);
    UpdateResponse resp;
    
    synchronized (cloudClient) { // nocommit: WTF? do we need sync or not? if so why cloudClient?
      // send updates to leader, to avoid SOLR-8733
      resp = ureq.process(LEADER);
    }
    
    long returnedVersion = Long.parseLong(((NamedList)resp.getResponse().get("adds")).getVal(0).toString());
    assertTrue("Due to SOLR-8733, sometimes returned version is 0. Let us assert that we have successfully"
        + " worked around that problem here.", returnedVersion > 0);
    return returnedVersion;
  }

  /**
   * Convinience method variant that never uses <code>initFloat</code>
   * @see #buildRandomIndex(Float,boolean,int[])
   */
  protected List<Long> buildRandomIndex(int... specialIds) throws Exception {
    return buildRandomIndex(null, false, specialIds);
  }
                                        
  /** 
   * Helper method to build a randomized index with the fields needed for all test methods in this class.
   * At a minimum, this index will contain 1 doc per "special" (non-negative) document id.  These special documents will be added with the <code>initFloat</code> specified in the "inplace_updatable_float" based on the <code>useFloatRandomly</code> param.
   *
   * A random number of documents (with negative ids) will be indexed in between each of the 
   * "special" documents, as well as before/after the first/last special document.
   *
   * @param initFloat Value to use in the "inplace_updatable_float" for some of the special documents, based on the <code>useFloatRandomly</code> param; will never be used if null
   * @param useFloatRandomly  If false, all special docs will get the <code>initFloat</code> value; if true, only a random subset of the special docs will get a value.
   * @param specialIds The ids to use for the special documents, all values must be non-negative
   * @return the versions of each of the specials document returned when indexing it
   */
  protected List<Long> buildRandomIndex(Float initFloat, boolean useFloatRandomly,
                                        int... specialIds) throws Exception {
    int id = -1; // used for non special docs
    final int numPreDocs = rarely() ? TestUtil.nextInt(random(),0,9) : atLeast(10);
    for (int i = 1; i <= numPreDocs; i++) {
      addDocAndGetVersion("id", id, "title_s", "title" + id, "id_i", id);
      id--;
    }
    final List<Long> versions = new ArrayList<>(specialIds.length);
    for (int special : specialIds) {
      if (null == initFloat || (useFloatRandomly && random().nextBoolean()) ) {
        versions.add(addDocAndGetVersion("id", special, "title_s", "title" + special, "id_i", special));
      } else {
        versions.add(addDocAndGetVersion("id", special, "title_s", "title" + special, "id_i", special,
                                         "inplace_updatable_float", initFloat));
      }
      final int numPostDocs = rarely() ? TestUtil.nextInt(random(),0,9) : atLeast(10);
      for (int i = 1; i <= numPostDocs; i++) {
        addDocAndGetVersion("id", id, "title_s", "title" + id, "id_i", id);
        id--;
      }
    }
    LEADER.commit();
    
    assert specialIds.length == versions.size();
    return versions;
  }
  
}
