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
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.cloud.SolrCloudTestCase;
import static org.apache.solr.cloud.TestTolerantUpdateProcessorCloud.assertUpdateTolerantErrors;
import static org.apache.solr.cloud.TestTolerantUpdateProcessorCloud.addErr;
import static org.apache.solr.cloud.TestTolerantUpdateProcessorCloud.delIErr;
import static org.apache.solr.cloud.TestTolerantUpdateProcessorCloud.delQErr;
import static org.apache.solr.cloud.TestTolerantUpdateProcessorCloud.f;
import static org.apache.solr.cloud.TestTolerantUpdateProcessorCloud.update;
import static org.apache.solr.cloud.TestTolerantUpdateProcessorCloud.ExpectedErr;
import org.apache.solr.client.solrj.SolrClient;
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
import org.apache.solr.common.ToleratedUpdateError;
import org.apache.solr.common.ToleratedUpdateError.CmdType;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.util.RevertDefaultThreadHandlerRule;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test of TolerantUpdateProcessor using a randomized MiniSolrCloud &amp; ChaosMokney.  
 * Reuses some utility methods in {@link TestTolerantUpdateProcessorCloud}
 *
 * <p>
 * <b>NOTE:</b> This test sets up a static instance of MiniSolrCloud with a single collection 
 * and several clients pointed at specific nodes. These are all re-used across multiple test methods, 
 * and assumes that the state of the cluster is healthy between tests.
 * </p>
 *
 */
public class TestTolerantUpdateProcessorChaosMonkey extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String COLLECTION_NAME = "test_col";
  
  /** A basic client for operations at the cloud level, default collection will be set */
  private static CloudSolrClient CLOUD_CLIENT;

  @BeforeClass
  private static void createMiniSolrCloudCluster() throws Exception {
    
    final String configName = "solrCloudCollectionConfig";
    final File configDir = new File(TEST_HOME() + File.separator + "collection1" + File.separator + "conf");

    final int numShards = TestUtil.nextInt(random(), 2, TEST_NIGHTLY ? 5 : 3);
    final int repFactor = TestUtil.nextInt(random(), 2, TEST_NIGHTLY ? 5 : 3);
    // at least one server won't have any replicas
    final int numServers = 1 + (numShards * repFactor);
    
    configureCluster(numServers)
      .addConfig(configName, configDir.toPath())
      .configure();
    
    Map<String, String> collectionProperties = new HashMap<>();
    collectionProperties.put("config", "solrconfig-distrib-update-processor-chains.xml");
    collectionProperties.put("schema", "schema15.xml"); // string id 

    // nocommit: SOLR-8862 why does this seed constantly cause createCollection to reliably complain that there are no live servers?!?!?!
    //
    // ant test  -Dtestcase=TestTolerantUpdateProcessorChaosMonkey -Dtests.seed=E73756F21DF21ECC -Dtests.slow=true -Dtests.locale=tr-TR -Dtests.timezone=America/Manaus -Dtests.asserts=true -Dtests.file.encoding=US-ASCII
    //
    //
    // [junit4]    > Throwable #1: org.apache.solr.client.solrj.SolrServerException: No live SolrServers available to handle this request:[http://127.0.0.1:43171/solr, http://127.0.0.1:49629/solr, http://127.0.0.1:59664/solr, http://127.0.0.1:40944/solr, http://127.0.0.1:38476/solr]
    // [junit4]    > 	at __randomizedtesting.SeedInfo.seed([E73756F21DF21ECC]:0)
    // [junit4]    > 	at org.apache.solr.client.solrj.impl.LBHttpSolrClient.request(LBHttpSolrClient.java:352)
    // [junit4]    > 	at org.apache.solr.client.solrj.impl.CloudSolrClient.sendRequest(CloudSolrClient.java:1157)
    // [junit4]    > 	at org.apache.solr.client.solrj.impl.CloudSolrClient.requestWithRetryOnStaleState(CloudSolrClient.java:927)
    // [junit4]    > 	at org.apache.solr.client.solrj.impl.CloudSolrClient.request(CloudSolrClient.java:863)
    // [junit4]    > 	at org.apache.solr.client.solrj.SolrClient.request(SolrClient.java:1219)
    // [junit4]    > 	at org.apache.solr.cloud.MiniSolrCloudCluster.makeCollectionsRequest(MiniSolrCloudCluster.java:415)
    // [junit4]    > 	at org.apache.solr.cloud.MiniSolrCloudCluster.createCollection(MiniSolrCloudCluster.java:399)
    //
    assertNotNull(cluster.createCollection(COLLECTION_NAME, numShards, repFactor,
                                           configName, null, null, collectionProperties));
    
    CLOUD_CLIENT = cluster.getSolrClient();
    CLOUD_CLIENT.setDefaultCollection(COLLECTION_NAME);

    ZkStateReader zkStateReader = CLOUD_CLIENT.getZkStateReader();
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(COLLECTION_NAME, zkStateReader, true, true, 330);

    // nocommit: init chaos monkey
    
  }
  
  @AfterClass
  private static void cleanup() throws Exception {
    // nocommit: shutdown the monkey
  }

  @Before
  private void deleteAllDocs() throws Exception {
    assertEquals(0, update(params("commit","true")).deleteByQuery("*:*").process(CLOUD_CLIENT).getStatus());
    assertEquals("index should be empty",
                 CLOUD_CLIENT.query(params("q","*:*","rows","0")).getResults().getNumFound());
  }
  
  public void testRandomUpdatesWithChaos() throws Exception {
    final int maxDocId = atLeast(10000);
    final BitSet expectedDocIds = new BitSet(maxDocId+1);
    
    final int numIters = atLeast(50);
    for (int i = 0; i < numIters; i++) {

      // nocommit: chaosMonkey.causeSomeChaos()
      
      final UpdateRequest req = update(params("maxErrors","-1",
                                              "update.chain", "tolerant-chain-max-errors-10"));
      final int numCmds = TestUtil.nextInt(random(), 1, 20);
      final List<ExpectedErr> expectedErrors = new ArrayList<ExpectedErr>(numCmds);
      int expectedErrorsCount = 0;
      // it's ambigious/confusing which order mixed DELQ + ADD  (or ADD and DELI for the same ID)
      // in the same request wll be processed by various clients, so we keep things simple
      // and ensure that no single doc Id is affected by more then one command in the same request
      final BitSet docsAffectedThisRequest = new BitSet(maxDocId+1);
      for (int cmdIter = 0; cmdIter < numCmds; cmdIter++) {
        final boolean causeError = random().nextBoolean();
        if (causeError) {
          expectedErrorsCount++;
        }
        
        if (random().nextBoolean()) {
          // add a doc
          final int id_i = randomUnsetBit(random(), docsAffectedThisRequest);
          final String id = "id_"+id_i;
          docsAffectedThisRequest.set(id_i);
          if (causeError) {
            expectedErrors.add(addErr(id));
          } else {
            expectedDocIds.set(id_i);
          }
          req.add(doc(f("id",id),
                      f("id_i", id_i),
                      f("foo_i", causeError ? "bogus_val" : TestUtil.nextInt(random(), 42, 666))));
          
        } else {
          // delete something
          if (random().nextBoolean()) {
            // delete by id
            final int id_i = randomUnsetBit(random(), docsAffectedThisRequest);
            final String id = "id_"+id_i;
            final boolean docExists = expectedDocIds.get(id_i);
            docsAffectedThisRequest.set(id_i);
            long versionConstraint = docExists ? 1 : -1;
            if (causeError) {
              versionConstraint = -1 * versionConstraint;
              expectedErrors.add(delIErr(id));
            } else {
              // if doc exists it will legitimately be deleted
              expectedDocIds.clear(id_i);
            }
            req.deleteById(id, versionConstraint);
            
          } else {
            // delete by query
            final String q;
            if (causeError) {
              // if our DBQ is going to fail, then it doesn't matter what q we use,
              // none of the docs in the index will be affected either way
              q = "foo_i:[42 TO ....giberish";
              expectedErrors.add(delQErr(q));
            } else {
              // ensure our DBQ is only over a range of docs not already affected
              // by any other cmds in this request
              final int rangeAxis = randomUnsetBit(random(), docsAffectedThisRequest);
              final int loBound = docsAffectedThisRequest.previousSetBit(rangeAxis);
              final int hiBound = docsAffectedThisRequest.nextSetBit(rangeAxis);
              final int lo = TestUtil.nextInt(random(), loBound+1, rangeAxis);
              final int hi = TestUtil.nextInt(random(), rangeAxis,
                                              // bound might be negative if no set bits above axis
                                              (hiBound < 0) ? maxDocId : hiBound-1);
              
              // NOTE: clear & set are exclusive of hi, so we use "}" in range query accordingly
              q = "id_i:[" + lo + " TO " + hi + (causeError ? "...giberish" : "}");
              expectedDocIds.clear(lo, hi);
              docsAffectedThisRequest.set(lo, hi);
            }
            req.deleteByQuery(q);
          }
        }
      }
      assertEquals("expected error count sanity check: " + req.toString(),
                   expectedErrorsCount, expectedErrors.size());
        
      // nocommit: 50% randomly: use an HttpSolrClient from the list of servers the monkey says are up
      final SolrClient client = CLOUD_CLIENT;

      UpdateResponse rsp = req.process(client);
      assertUpdateTolerantErrors(expectedErrors.toString(), rsp,
                                 expectedErrors.toArray(new ExpectedErr[expectedErrors.size()]));
      
    }
    assertEquals("commit failed?", 0, CLOUD_CLIENT.commit().getStatus());
    
    assertEquals("final doc count doesn't match bitself cardinality",
                 expectedDocIds.cardinality(),
                 CLOUD_CLIENT.query(params("q","*:*","rows","0")).getResults().getNumFound());
  }

  /** sanity check that randomUnsetBit works as expected 
   * @see #randomUnsetBit
   */
  public void testSanityRandomUnsetBit() {
    BitSet bits = new BitSet(atLeast(100));
    int nextBit = -2;
    for (int i = 0; i < bits.size(); i++) {
      assertFalse("how is bitset already full? iter="+i+" card="+bits.cardinality()+"/size="+bits.size(),
                  bits.cardinality() == bits.size());
      nextBit = randomUnsetBit(random(), bits);
      assertTrue("nextBit shouldn't be negative yet: " + nextBit,
                 0 <= nextBit);
      assertFalse("expect unset: " + nextBit, bits.get(nextBit));
      bits.set(nextBit);
    }
    assertEquals("why isn't bitset full?", bits.size(), bits.cardinality());
    nextBit = randomUnsetBit(random(), bits);
    assertEquals("wrong nextBit at end of all iters", -1, nextBit);
    nextBit = randomUnsetBit(random(), bits);
    assertEquals("wrong nextBit at redundent end of all iters", -1, nextBit);
  }
  
  public static SolrInputDocument doc(SolrInputField... fields) {
    // SolrTestCaseJ4 has same method name, prevents static import from working
    return TestTolerantUpdateProcessorCloud.doc(fields);
  }

  /**
   * Given a BitSet, returns a random bit that is currently false, or -1 if all bits are true.
   * NOTE: this method is not fair.
   */
  public static final int randomUnsetBit(Random r, BitSet bits) {
    if (bits.cardinality() == bits.size()) {
      return -1;
    }
    final int candidate = TestUtil.nextInt(r, 0, bits.size()-1);
    if (bits.get(candidate)) {
      final int lo = bits.previousClearBit(candidate);
      final int hi = bits.nextClearBit(candidate);
      if (lo < 0 && bits.size() <= hi) {
        fail("how the hell did we not short circut out? card="+bits.cardinality()+"/size="+bits.size());
      } else if (lo < 0) {
        return hi;
      } else if (bits.size() <= hi) {
        return lo;
      } // else...
      return ((candidate - lo) < (hi - candidate)) ? lo : hi;
    }
    return candidate;
  }
  
}
