
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

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.math3.primes.Primes;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.zookeeper.KeeperException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slow
public class TestStressInPlaceUpdates extends AbstractFullDistribZkTestBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeSuperClass() throws Exception {
    
    schemaString = "schema-inplace-updates.xml";
    configString = "solrconfig-tlog.xml";

    // sanity check that autocommits are disabled
    initCore(configString, schemaString);
    assertEquals(-1, h.getCore().getSolrConfig().getUpdateHandlerInfo().autoCommmitMaxTime);
    assertEquals(-1, h.getCore().getSolrConfig().getUpdateHandlerInfo().autoSoftCommmitMaxTime);
    assertEquals(-1, h.getCore().getSolrConfig().getUpdateHandlerInfo().autoCommmitMaxDocs);
    assertEquals(-1, h.getCore().getSolrConfig().getUpdateHandlerInfo().autoSoftCommmitMaxDocs);
  }

  public TestStressInPlaceUpdates() {
    super();
    sliceCount = 1;
    fixShardCount(3);
  }

  protected final ConcurrentHashMap<Integer, DocInfo> model = new ConcurrentHashMap<>();
  protected Map<Integer, DocInfo> committedModel = new HashMap<>();
  protected long snapshotCount;
  protected long committedModelClock;
  protected int clientIndexUsedForCommit;
  protected volatile int lastId;
  protected final String field = "val_l";

  private void initModel(int ndocs) {
    for (int i = 0; i < ndocs; i++) {
      model.put(i, new DocInfo(0l, 0, 0));
    }
    committedModel.putAll(model);
  }

  SolrClient leaderClient = null;

  @Test
  @ShardsFixed(num = 3)
  public void stressTest() throws Exception {
    waitForRecoveriesToFinish(true);

    this.leaderClient = getClientForLeader();
    assertNotNull("Couldn't obtain client for the leader of the shard", this.leaderClient);

    final int commitPercent = 5 + random().nextInt(20);
    final int softCommitPercent = 30 + random().nextInt(75); // what percent of the commits are soft
    final int deletePercent = 4 + random().nextInt(25);
    final int deleteByQueryPercent = random().nextInt(8);
    final int ndocs = 5 + (random().nextBoolean() ? random().nextInt(25) : random().nextInt(200));
    int nWriteThreads = 5 + random().nextInt(25);
    int fullUpdatePercent = 5 + random().nextInt(50);

    // query variables
    final int percentRealtimeQuery = 75;
    // number of cumulative read/write operations by all threads
    final AtomicLong operations = new AtomicLong(25000);  
    int nReadThreads = 5 + random().nextInt(25);


    /** // testing
     final int commitPercent = 5;
     final int softCommitPercent = 100; // what percent of the commits are soft
     final int deletePercent = 0;
     final int deleteByQueryPercent = 50;
     final int ndocs = 10;
     int nWriteThreads = 10;

     final int maxConcurrentCommits = nWriteThreads;   // number of committers at a time... it should be <= maxWarmingSearchers

     // query variables
     final int percentRealtimeQuery = 101;
     final AtomicLong operations = new AtomicLong(50000);  // number of query operations to perform in total
     int nReadThreads = 10;

     int fullUpdatePercent = 20;
     **/

    log.info("{}", Arrays.asList
             ("commitPercent", commitPercent, "softCommitPercent", softCommitPercent,
              "deletePercent", deletePercent, "deleteByQueryPercent", deleteByQueryPercent,
              "ndocs", ndocs, "nWriteThreads", nWriteThreads, "percentRealtimeQuery", percentRealtimeQuery,
              "operations", operations, "nReadThreads", nReadThreads));

    initModel(ndocs);

    List<Thread> threads = new ArrayList<>();

    for (int i = 0; i < nWriteThreads; i++) {
      Thread thread = new Thread("WRITER" + i) {
        Random rand = new Random(random().nextInt());

        @Override
        public void run() {
          try {
            while (operations.decrementAndGet() > 0) {
              int oper = rand.nextInt(100);

              if (oper < commitPercent) {
                Map<Integer, DocInfo> newCommittedModel;
                long version;

                synchronized (TestStressInPlaceUpdates.this) {
                  // take a snapshot of the model
                  // this is safe to do w/o synchronizing on the model because it's a ConcurrentHashMap
                  newCommittedModel = new HashMap<>(model);  
                  version = snapshotCount++;

                  int chosenClientIndex = rand.nextInt(clients.size());

                  if (rand.nextInt(100) < softCommitPercent) {
                    log.info("softCommit start");
                    clients.get(chosenClientIndex).commit(true, true, true);
                    log.info("softCommit end");
                  } else {
                    log.info("hardCommit start");
                    clients.get(chosenClientIndex).commit();
                    log.info("hardCommit end");
                  }

                  // install this model snapshot only if it's newer than the current one
                  if (version >= committedModelClock) {
                    if (VERBOSE) {
                      log.info("installing new committedModel version={}", committedModelClock);
                    }
                    clientIndexUsedForCommit = chosenClientIndex;
                    committedModel = newCommittedModel;
                    committedModelClock = version;
                  }
                }
                continue;
              }

              int id;

              if (rand.nextBoolean()) {
                id = rand.nextInt(ndocs);
              } else {
                id = lastId;  // reuse the last ID half of the time to force more race conditions
              }

              // set the lastId before we actually change it sometimes to try and
              // uncover more race conditions between writing and reading
              boolean before = rand.nextBoolean();
              if (before) {
                lastId = id;
              }

              DocInfo info = model.get(id);

              // yield after getting the next version to increase the odds of updates happening out of order
              if (rand.nextBoolean()) Thread.yield();

              if (oper < commitPercent + deletePercent + deleteByQueryPercent) {
                log.info("deleting id {}: {}",id,info);
                
                long returnedVersion;

                try {
                  final boolean dbq = (oper >= commitPercent + deletePercent);
                  returnedVersion = deleteDocAndGetVersion(Integer.toString(id), params("_version_", Long.toString(info.version)), dbq);
                  log.info((dbq? "DBI": "DBQ")+": Deleting id=" + id + "], version=" + info.version 
                      + ".  Returned version=" + returnedVersion);
                } catch (RuntimeException e) {
                  if (e.getMessage() != null && e.getMessage().contains("version conflict")
                      || e.getMessage() != null && e.getMessage().contains("Conflict")) {
                    // Its okay for a leader to reject a concurrent request
                    log.warn("Conflict during partial update, rejected id=" + id + ", " + e);
                    returnedVersion = -1;
                  } else throw e;
                }

                // only update model if the version is newer
                synchronized (model) {
                  DocInfo currInfo = model.get(id);
                  if (Math.abs(returnedVersion) > Math.abs(currInfo.version)) {
                    model.put(id, new DocInfo(returnedVersion, 0, 0));
                  }
                }
                
              } else {
                int val1 = info.intFieldValue;
                long val2 = info.longFieldValue;
                int nextVal1 = val1;
                long nextVal2 = val2;

                int addOper = rand.nextInt(100);
                long returnedVersion;
                if (addOper < fullUpdatePercent || info.version <= 0) { // if document was never indexed or was deleted
                  // FULL UPDATE
                  nextVal1 = Primes.nextPrime(val1 + 1);
                  nextVal2 = nextVal1 * 1000000000l;
                  try {
                    returnedVersion = addDocAndGetVersion("id", id, "title_s", "title" + id, "val1_i_dvo", nextVal1, "val2_l_dvo", nextVal2, "_version_", info.version);
                    log.info("FULL: Writing id=" + id + ", val=[" + nextVal1 + "," + nextVal2 + "], version=" + info.version + ", Prev was=[" + val1 + "," + val2 + "].  Returned version=" + returnedVersion);

                  } catch (RuntimeException e) {
                    if (e.getMessage() != null && e.getMessage().contains("version conflict")
                        || e.getMessage() != null && e.getMessage().contains("Conflict")) {
                      // Its okay for a leader to reject a concurrent request
                      log.warn("Conflict during partial update, rejected id=" + id + ", " + e);
                      returnedVersion = Integer.MIN_VALUE;
                    } else throw e;
                  }
                } else {
                  // PARTIAL
                  nextVal2 = val2 + val1;
                  try {
                    returnedVersion = addDocAndGetVersion("id", id, "val2_l_dvo", map("inc", String.valueOf(val1)), "_version_", info.version);
                    log.info("PARTIAL: Writing id=" + id + ", val=[" + nextVal1 + "," + nextVal2 + "], version=" + info.version + ", Prev was=[" + val1 + "," + val2 + "].  Returned version=" + returnedVersion);
                  } catch (RuntimeException e) {
                    if (e.getMessage() != null && e.getMessage().contains("version conflict")
                        || e.getMessage() != null && e.getMessage().contains("Conflict")) {
                      // Its okay for a leader to reject a concurrent request
                      log.warn("Conflict during full update, rejected id=" + id + ", " + e);
                      returnedVersion = -1;
                    } else if (e.getMessage() != null && e.getMessage().contains("Document not found for update.") 
                        && e.getMessage().contains("id="+id)) {
                      log.warn("Attempting a partial update for a recently deleted document, rejected id=" + id + ", " + e);
                      returnedVersion = Integer.MIN_VALUE;
                    } else throw e;
                  }
                }

                // only update model if the version is newer
                synchronized (model) {
                  DocInfo currInfo = model.get(id);
                  if (returnedVersion > currInfo.version) {
                    model.put(id, new DocInfo(returnedVersion, nextVal1, nextVal2));
                  }

                }
              }

              if (!before) {
                lastId = id;
              }
            }
          } catch (Throwable e) {
            operations.set(-1L);
            log.error("", e);
            throw new RuntimeException(e);
          }
        }
      };

      threads.add(thread);

      // nocommit: need a final pass over the model to check every doc
    }

    // Read threads
    for (int i = 0; i < nReadThreads; i++) {
      Thread thread = new Thread("READER" + i) {
        Random rand = new Random(random().nextInt());

        @SuppressWarnings("unchecked")
        @Override
        public void run() {
          try {
            while (operations.decrementAndGet() >= 0) {
              // bias toward a recently changed doc
              int id = rand.nextInt(100) < 25 ? lastId : rand.nextInt(ndocs);

              // when indexing, we update the index, then the model
              // so when querying, we should first check the model, and then the index

              boolean realTime = rand.nextInt(100) < percentRealtimeQuery;
              DocInfo info;

              if (realTime) {
                info = model.get(id);
              } else {
                synchronized (TestStressInPlaceUpdates.this) {
                  info = committedModel.get(id);
                }
              }

              if (VERBOSE) {
                log.info("querying id {}", id);
              }
              ModifiableSolrParams params = new ModifiableSolrParams();
              if (realTime) {
                params.set("wt", "json");
                params.set("qt", "/get");
                params.set("ids", Integer.toString(id));
              } else {
                params.set("wt", "json");
                params.set("q", "id:" + Integer.toString(id));
                params.set("omitHeader", "true");
              }

              int clientId = rand.nextInt(clients.size());
              if (!realTime) clientId = clientIndexUsedForCommit;

              QueryResponse response = clients.get(clientId).query(params);
              if (response.getResults().size() == 0) {
                // there's no info we can get back with a delete, so not much we can check without further synchronization
              } else if (response.getResults().size() == 1) {
                assertNotNull("Realtime=" + realTime + ", Response is: " + response + ", model: " + info,
                    response.getResults().get(0).get("val2_l_dvo"));
                assertNotNull("Realtime=" + realTime + ", Response is: " + response.getResults().get(0) + ", model: " + info + ", client="+((HttpSolrClient)clients.get(clientId)).getBaseURL()+", leaderClient="+((HttpSolrClient)leaderClient).getBaseURL(),
                    response.getResults().get(0).get("val1_i_dvo"));

                Object obj1 = response.getResults().get(0).getFirstValue("val1_i_dvo");
                int val1 = (Integer) obj1;
                Object obj2 = response.getResults().get(0).getFirstValue("val2_l_dvo");
                long val2 = (Long) obj2;
                Object objVer = response.getResults().get(0).getFirstValue("_version_");
                long foundVer = (Long) objVer;


                if (!(val1 == 0 && val2 == 0 || val2 % val1 == 0)) {
                  assertTrue("Vals are: " + val1 + ", " + val2 + ", id=" + id + ", clientId=" + clients.get(clientId) + ", Doc retrived is: " + response.toString(),
                      val1 == 0 && val2 == 0 || val2 % val1 == 0);

                }
                if (foundVer < Math.abs(info.version)
                    || (foundVer == info.version && (val1 != info.intFieldValue || val2 != info.longFieldValue))) {    // if the version matches, the val must
                  log.error("Realtime=" + realTime + ", ERROR, id=" + id + " found=" + response + " model=" + info + ", client="+((HttpSolrClient)clients.get(clientId)).getBaseURL()+", leaderClient="+((HttpSolrClient)leaderClient).getBaseURL());
                  assertTrue("Realtime=" + realTime + ", ERROR, id=" + id + " found=" + response + " model=" + info, false);
                }
              } else {
                fail(String.format(Locale.ENGLISH, "There were more than one result: {}", response));
              }
            }
          } catch (Throwable e) {
            operations.set(-1L);
            log.error("", e);
            throw new RuntimeException(e);
          }
        }
      };

      threads.add(thread);
    }
    // Start all threads
    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }
  }

  /**
   * Used for storing the info for a document in an in-memory model.
   */
  private static class DocInfo {
    long version;
    int intFieldValue;
    long longFieldValue;

    public DocInfo(long version, int val1, long val2) {
      this.version = version;
      this.intFieldValue = val1;
      this.longFieldValue = val2;
    }

    @Override
    public String toString() {
      return "[version=" + version + ", intValue=" + intFieldValue + ",longValue=" + longFieldValue + "]";
    }
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

    // send updates to leader, to avoid SOLR-8733
    resp = ureq.process(leaderClient);

    long returnedVersion = Long.parseLong(((NamedList) resp.getResponse().get("adds")).getVal(0).toString());
    assertTrue("Due to SOLR-8733, sometimes returned version is 0. Let us assert that we have successfully"
        + " worked around that problem here.", returnedVersion > 0);
    return returnedVersion;
  }

  @SuppressWarnings("rawtypes")
  protected long deleteDocAndGetVersion(String id, ModifiableSolrParams params, boolean deleteByQuery) throws Exception {
    params.add("versions", "true");
   
    UpdateRequest ureq = new UpdateRequest();
    ureq.setParams(params);
    if (deleteByQuery) {
      ureq.deleteByQuery("id:"+id);
    } else {
      ureq.deleteById(id);
    }
    UpdateResponse resp;
    // send updates to leader, to avoid SOLR-8733
    resp = ureq.process(leaderClient);
    
    String key = deleteByQuery? "deleteByQuery": "deletes";
    long returnedVersion = Long.parseLong(((NamedList) resp.getResponse().get(key)).getVal(0).toString());
    assertTrue("Due to SOLR-8733, sometimes returned version is 0. Let us assert that we have successfully"
        + " worked around that problem here.", returnedVersion < 0);
    return returnedVersion;
  }

  /**
   * Method gets the SolrClient for the leader replica. This is needed for a workaround for SOLR-8733.
   */
  public SolrClient getClientForLeader() throws KeeperException, InterruptedException {
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    cloudClient.getZkStateReader().forceUpdateCollection(DEFAULT_COLLECTION);
    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    Replica leader = null;
    Slice shard1 = clusterState.getCollection(DEFAULT_COLLECTION).getSlice(SHARD1);
    leader = shard1.getLeader();

    for (int i = 0; i < clients.size(); i++) {
      String leaderBaseUrl = zkStateReader.getBaseUrlForNodeName(leader.getNodeName());
      if (((HttpSolrClient) clients.get(i)).getBaseURL().startsWith(leaderBaseUrl))
        return clients.get(i);
    }

    return null;
  }
}
