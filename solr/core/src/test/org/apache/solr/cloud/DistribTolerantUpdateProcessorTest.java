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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.HashSet;
import java.util.Set;


import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.junit.Test;

public class DistribTolerantUpdateProcessorTest extends AbstractFullDistribZkTestBase {
  

  @Override
  protected String getCloudSolrConfig() {
    return "solrconfig-distrib-update-processor-chains.xml";
  }

  @ShardsFixed(num = 2)
  public void testValidAdds() throws Exception {
    del("*:*");
    handle.clear();
    handle.put("maxScore", SKIPVAL);
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    SolrInputDocument validDoc = sdoc("id", "1", "text", "the quick brown fox");
    indexDoc("tolerant-chain-max-errors-10", validDoc);
    validDoc = sdoc("id", "2", "text", "the quick brown fox");
    indexDoc("tolerant-chain-max-errors-not-set", validDoc);

    commit();
    ModifiableSolrParams query = new ModifiableSolrParams();
    query.add("q", "*:*");
    QueryResponse queryResponse = queryServer(query);
    SolrDocumentList solrDocuments = queryResponse.getResults();
    assertEquals("Expected to match 2 docs", solrDocuments.size(), 2);

    query = new ModifiableSolrParams();
    query.add("q", "id:1");
    queryResponse = queryServer(query);
    solrDocuments = queryResponse.getResults();
    assertEquals("Expected to match 1 doc", solrDocuments.size(), 1);

    query = new ModifiableSolrParams();
    query.add("q", "id:2");
    queryResponse = queryServer(query);
    solrDocuments = queryResponse.getResults();
    assertEquals("Expected to match 1 doc", solrDocuments.size(), 1);
  }

  @Test
  @ShardsFixed(num = 2)
  public void testInvalidAdds() throws Exception {
    //del("*:*");
    handle.clear();
    handle.put("maxScore", SKIPVAL);
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    SolrInputDocument invalidDoc = sdoc("id", 1, "iind", "b");
    try {
      // This doc should fail without being tolerant
      indexDoc("not-tolerant", invalidDoc);
      fail("Expecting exception");
    } catch (SolrException e) {
      // we can't make any reliable assertions about the error message, because
      // it varies based on how the request was routed
      // nocommit: can we tighten this any more?
      assertTrue("not the type of error we were expecting: " + e.toString(),
                 400 <= e.code() && e.code() < 500);
    }
    assertUSucceedsWithErrors("tolerant-chain-max-errors-10",
                              new SolrInputDocument[]{ invalidDoc,
                                                      sdoc("id", 4, "text", "the brown fox") },
                              null, 1, "1");
    commit();

    ModifiableSolrParams query = new ModifiableSolrParams();
    query.add("q", "id:4");
    QueryResponse queryResponse = queryServer(query);
    SolrDocumentList solrDocuments = queryResponse.getResults();
    assertEquals("Expected to match 1 doc", 1, solrDocuments.size());

  }

  // nocommit: redesign so that we can assert errors of diff types besides "add" (ie: deletes) 
  private void assertUSucceedsWithErrors(String chain, SolrInputDocument[] docs,
                                         SolrParams requestParams,
                                         int numErrors,
                                         String... idsShouldFail) throws Exception {
    
    // nocommit: retire numErrors from this method sig ... trappy
    assertEquals("bad test, idsShouldFail.length doesn't match numErrors",
                 numErrors, idsShouldFail.length);
    
    ModifiableSolrParams newParams = new ModifiableSolrParams(requestParams);
    newParams.set("update.chain", chain);
    UpdateResponse response = indexDoc(newParams, docs);
    @SuppressWarnings("unchecked")
    List<SimpleOrderedMap<String>> errors = (List<SimpleOrderedMap<String>>)
      response.getResponseHeader().get("errors");
    assertNotNull("Null errors in response: " + response.toString(), errors);

    assertEquals("number of errors in response: " + response.toString(), idsShouldFail.length, errors.size());
    
    Set<String> addErrorIdsExpected = new HashSet<String>(Arrays.asList(idsShouldFail));
    
    for (SimpleOrderedMap<String> err : errors) {
      // nocommit: support other types
      assertEquals("nocommit: error type not handled yet by this method",
                   "ADD", err.get("type"));
      
      String id = err.get("id");
      assertNotNull("null err id", id);
      assertTrue("unexpected id in errors list: " + response.toString(),
                 addErrorIdsExpected.contains(id));
    }
    
  }

  @Override
  protected void setDistributedParams(ModifiableSolrParams params) {
    //don't add anything, let SolrCloud handle this
  }

  private UpdateResponse indexDoc(String updateChain, SolrInputDocument doc) throws IOException, SolrServerException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("update.chain", updateChain);
    return indexDoc(params, doc);
  }

  private UpdateResponse indexDoc(SolrParams params, SolrInputDocument... docs)
      throws IOException, SolrServerException {
    int which = random().nextInt(clients.size());
    SolrClient client = clients.get(which);
    return add(client, params, docs);
  }
}
