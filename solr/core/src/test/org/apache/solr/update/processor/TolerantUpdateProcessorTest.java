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
package org.apache.solr.update.processor;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.xml.xpath.XPathExpressionException;

import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.servlet.DirectSolrConnection;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.TolerantUpdateProcessor.KnownErr;
import org.apache.solr.update.processor.TolerantUpdateProcessor.CmdType;
import org.apache.solr.util.BaseTestHarness;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.SAXException;

public class TolerantUpdateProcessorTest extends UpdateProcessorTestBase {
  
  /**
   * List of valid + invalid documents
   */
  private static List<SolrInputDocument> docs = null;
  /**
   * IDs of the invalid documents in <code>docs</code>
   */
  private static String[] badIds = null;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-update-processor-chains.xml", "schema12.xml");
  }
  
  @AfterClass
  public static void tearDownClass() {
    docs = null;
    badIds = null;
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    //expected exception messages
    ignoreException("Error adding field");
    ignoreException("Document is missing mandatory uniqueKey field");
    if (docs == null) {
      docs = new ArrayList<>(20);
      badIds = new String[10];
      for(int i = 0; i < 10;i++) {
        // a valid document
        docs.add(doc(field("id", 1f, String.valueOf(2*i)), field("weight", 1f, i)));
        // ... and an invalid one
        docs.add(doc(field("id", 1f, String.valueOf(2*i+1)), field("weight", 1f, "b")));
        badIds[i] = String.valueOf(2*i+1);
      }
    }
    
  }
  
  @Override
  public void tearDown() throws Exception {
    resetExceptionIgnores();
    assertU(delQ("*:*"));
    assertU(commit());
    assertQ(req("q","*:*")
        ,"//result[@numFound='0']");
    super.tearDown();
  }
  
  @Test
  public void testValidAdds() throws IOException {
    SolrInputDocument validDoc = doc(field("id", 1f, "1"), field("text", 1f, "the quick brown fox"));
    add("tolerant-chain-max-errors-10", null, validDoc);
    
    validDoc = doc(field("id", 1f, "2"), field("text", 1f, "the quick brown fox"));
    add("tolerant-chain-max-errors-not-set", null, validDoc);
    
    assertU(commit());
    assertQ(req("q","*:*")
        ,"//result[@numFound='2']");
    assertQ(req("q","id:1")
        ,"//result[@numFound='1']");
    assertQ(req("q","id:2")
        ,"//result[@numFound='1']");
  }
  
  @Test
  public void testInvalidAdds() throws IOException {
    SolrInputDocument invalidDoc = doc(field("text", 1f, "the quick brown fox")); //no id
    try {
      // This doc should fail without being tolerant
      add("not-tolerant", null, invalidDoc);
      fail("Expecting exception");
    } catch (Exception e) {
      //expected
      assertTrue(e.getMessage().contains("Document is missing mandatory uniqueKey field"));
    }
    assertUSucceedsWithErrors("tolerant-chain-max-errors-10", Arrays.asList(new SolrInputDocument[]{invalidDoc}), null, 1, "(unknown)");
    
    //a valid doc
    SolrInputDocument validDoc = doc(field("id", 1f, "1"), field("text", 1f, "the quick brown fox"));
    
    try {
      // This batch should fail without being tolerant
      add("not-tolerant", null, Arrays.asList(new SolrInputDocument[]{invalidDoc, validDoc}));
      fail("Expecting exception");
    } catch (Exception e) {
      //expected
      assertTrue(e.getMessage().contains("Document is missing mandatory uniqueKey field"));
    }
    
    assertU(commit());
    assertQ(req("q","id:1")
        ,"//result[@numFound='0']");
    
    
    assertUSucceedsWithErrors("tolerant-chain-max-errors-10", Arrays.asList(new SolrInputDocument[]{invalidDoc, validDoc}), null, 1, "(unknown)");
    assertU(commit());
    
    // verify that the good document made it in. 
    assertQ(req("q","id:1")
        ,"//result[@numFound='1']");
    
    invalidDoc = doc(field("id", 1f, "2"), field("weight", 1f, "aaa"));
    validDoc = doc(field("id", 1f, "3"), field("weight", 1f, "3"));
    
    try {
      // This batch should fail without being tolerant
      add("not-tolerant", null, Arrays.asList(new SolrInputDocument[]{invalidDoc, validDoc})); //no id
      fail("Expecting exception");
    } catch (Exception e) {
      //expected
      assertTrue(e.getMessage().contains("Error adding field"));
    }
    
    assertU(commit());
    assertQ(req("q","id:3")
        ,"//result[@numFound='0']");
    
    assertUSucceedsWithErrors("tolerant-chain-max-errors-10", Arrays.asList(new SolrInputDocument[]{invalidDoc, validDoc}), null, 1, "2");
    assertU(commit());
    
    // The valid document was indexed
    assertQ(req("q","id:3")
        ,"//result[@numFound='1']");
    
    // The invalid document was NOT indexed
    assertQ(req("q","id:2")
        ,"//result[@numFound='0']");
    
  }
  
  @Test
  public void testMaxErrorsDefault() throws IOException {
    try {
      // by default the TolerantUpdateProcessor accepts all errors, so this batch should succeed with 10 errors.
      assertUSucceedsWithErrors("tolerant-chain-max-errors-not-set", docs, null, 10, badIds);
    } catch(Exception e) {
      fail("Shouldn't get an exception for this batch: " + e.getMessage());
    }
    assertU(commit());
    assertQ(req("q","*:*")
        ,"//result[@numFound='10']");
  }
  
  public void testMaxErrorsSucceed() throws IOException {
    ModifiableSolrParams requestParams = new ModifiableSolrParams();
    requestParams.add("maxErrors", "10");
    // still OK
    assertUSucceedsWithErrors("tolerant-chain-max-errors-not-set", docs, requestParams, 10, badIds);
    assertU(commit());
    assertQ(req("q","*:*")
        ,"//result[@numFound='10']");
  }
  
  @Test
  public void testMaxErrorsThrowsException() throws IOException {
    ModifiableSolrParams requestParams = new ModifiableSolrParams();
    requestParams.add("maxErrors", "5");
    try {
      // should fail
      assertUSucceedsWithErrors("tolerant-chain-max-errors-not-set", docs, requestParams, 10, badIds);
      fail("Expecting exception");
    } catch (SolrException e) {
      assertTrue(e.getMessage(),
                 e.getMessage().contains("ERROR: [doc=1] Error adding field 'weight'='b' msg=For input string: \"b\""));
    }
    //the first good documents made it to the index
    assertU(commit());
    assertQ(req("q","*:*")
        ,"//result[@numFound='6']");
  }

  // nocommit: need a testMaxErrorsNegative (ie: infinite)
  
  @Test
  public void testMaxErrors0() throws IOException {
    //make the TolerantUpdateProcessor intolerant
    List<SolrInputDocument> smallBatch = docs.subList(0, 2);
    ModifiableSolrParams requestParams = new ModifiableSolrParams();
    requestParams.add("maxErrors", "0");
    try {
      // should fail
      assertUSucceedsWithErrors("tolerant-chain-max-errors-10", smallBatch, requestParams, 1, "1");
      fail("Expecting exception");
    } catch (SolrException e) {
      assertTrue(e.getMessage().contains("ERROR: [doc=1] Error adding field 'weight'='b' msg=For input string: \"b\""));
    }
    //the first good documents made it to the index
    assertU(commit());
    assertQ(req("q","*:*")
        ,"//result[@numFound='1']");
  }
  
  @Test
  public void testInvalidDelete() throws XPathExpressionException, SAXException {
    ignoreException("undefined field invalidfield");
    String response = update("tolerant-chain-max-errors-10", adoc("id", "1", "text", "the quick brown fox"));
    assertNull(BaseTestHarness.validateXPath(response, "//int[@name='status']=0",
        "//int[@name='numErrors']=0"));
    
    response = update("tolerant-chain-max-errors-10", delQ("invalidfield:1"));
    assertNull(BaseTestHarness.validateXPath(response, "//int[@name='status']=0",
        "//int[@name='numErrors']=1",
        "//lst[@name='errors']/lst[@name='invalidfield:1']",
        "//lst[@name='errors']/lst[@name='invalidfield:1']/str[@name='message']/text()='undefined field invalidfield'"));
  }
  
  @Test
  public void testValidDelete() throws XPathExpressionException, SAXException {
    ignoreException("undefined field invalidfield");
    String response = update("tolerant-chain-max-errors-10", adoc("id", "1", "text", "the quick brown fox"));
    assertNull(BaseTestHarness.validateXPath(response, "//int[@name='status']=0",
        "//int[@name='numErrors']=0"));
    assertU(commit());
    assertQ(req("q","*:*")
        ,"//result[@numFound='1']");
    
    response = update("tolerant-chain-max-errors-10", delQ("id:1"));
    assertNull(BaseTestHarness.validateXPath(response, "//int[@name='status']=0",
        "//int[@name='numErrors']=0"));
    assertU(commit());
    assertQ(req("q","*:*")
        ,"//result[@numFound='0']");
  }
  
  @Test
  public void testResponse() throws SAXException, XPathExpressionException, IOException {
    String response = update("tolerant-chain-max-errors-10", adoc("id", "1", "text", "the quick brown fox"));
    assertNull(BaseTestHarness.validateXPath(response, "//int[@name='status']=0",
        "//int[@name='numErrors']=0"));
    response = update("tolerant-chain-max-errors-10", adoc("text", "the quick brown fox"));
    assertNull(BaseTestHarness.validateXPath(response, "//int[@name='status']=0",
        "//int[@name='numErrors']=1",
        "//lst[@name='errors']/lst[@name='(unknown)']",
        "//lst[@name='errors']/lst[@name='(unknown)']/str[@name='message']/text()='Document is missing mandatory uniqueKey field: id'"));
    
    response = update("tolerant-chain-max-errors-10", adoc("text", "the quick brown fox"));
    StringWriter builder = new StringWriter();
    builder.append("<add>");
    for (SolrInputDocument doc:docs) {
      ClientUtils.writeXML(doc, builder);
    }
    builder.append("</add>");
    response = update("tolerant-chain-max-errors-10", builder.toString());
    assertNull(BaseTestHarness.validateXPath(response, "//int[@name='status']=0",
        "//int[@name='numErrors']=10",
        "not(//lst[@name='errors']/lst[@name='0'])",
        "//lst[@name='errors']/lst[@name='1']",
        "not(//lst[@name='errors']/lst[@name='2'])",
        "//lst[@name='errors']/lst[@name='3']",
        "not(//lst[@name='errors']/lst[@name='4'])",
        "//lst[@name='errors']/lst[@name='5']",
        "not(//lst[@name='errors']/lst[@name='6'])",
        "//lst[@name='errors']/lst[@name='7']",
        "not(//lst[@name='errors']/lst[@name='8'])",
        "//lst[@name='errors']/lst[@name='9']",
        "not(//lst[@name='errors']/lst[@name='10'])",
        "//lst[@name='errors']/lst[@name='11']",
        "not(//lst[@name='errors']/lst[@name='12'])",
        "//lst[@name='errors']/lst[@name='13']",
        "not(//lst[@name='errors']/lst[@name='14'])",
        "//lst[@name='errors']/lst[@name='15']",
        "not(//lst[@name='errors']/lst[@name='16'])",
        "//lst[@name='errors']/lst[@name='17']",
        "not(//lst[@name='errors']/lst[@name='18'])",
        "//lst[@name='errors']/lst[@name='19']"));
    
  }

  public void testKnownErrClass() {

    assertNull(KnownErr.parseMetadataIfKnownErr("some other key", "some value"));

    for (KnownErr in : new KnownErr[] {
        new KnownErr(CmdType.ADD, "doc1", "some error"),
        new KnownErr(CmdType.DELID, "doc1", "some diff error"),
        new KnownErr(CmdType.DELQ, "-field:yakko other_field:wakko", "some other error"),
      }) {
      KnownErr out = KnownErr.parseMetadataIfKnownErr(in.getMetadataKey(), in.getMetadataValue());
      assertNotNull(out);
      assertEquals(out.type, in.type);
      assertEquals(out.id, in.id);
      assertEquals(out.errorValue, in.errorValue);
      assertEquals(out.hashCode(), in.hashCode());
      assertEquals(out.toString(), in.toString());
      
      assertEquals(out, in);
      assertEquals(in, out);

    }
    
    assertFalse((new KnownErr(CmdType.ADD, "doc1", "some error")).equals
                (new KnownErr(CmdType.ADD, "doc2", "some error")));
    assertFalse((new KnownErr(CmdType.ADD, "doc1", "some error")).equals
                (new KnownErr(CmdType.ADD, "doc1", "some errorxx")));
    assertFalse((new KnownErr(CmdType.ADD, "doc1", "some error")).equals
                (new KnownErr(CmdType.DELID, "doc1", "some error")));
    

    // nocommit: add randomized testing, particularly with non-trivial 'id' values
    
  }

  
  public String update(String chain, String xml) {
    DirectSolrConnection connection = new DirectSolrConnection(h.getCore());
    SolrRequestHandler handler = h.getCore().getRequestHandler("/update");
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("update.chain", chain);
    try {
      return connection.request(handler, params, xml);
    } catch (SolrException e) {
      throw (SolrException)e;
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }
  }
  
  private void assertUSucceedsWithErrors(String chain, final Collection<SolrInputDocument> docs, SolrParams requestParams, int numErrors, String... ids) throws IOException {
    SolrQueryResponse response = add(chain, requestParams, docs);
    @SuppressWarnings("unchecked")
    SimpleOrderedMap<Object> errors = (SimpleOrderedMap<Object>) response.getResponseHeader().get("errors");
    assertNotNull(errors);
    assertEquals(numErrors, response.getResponseHeader().get("numErrors"));
    
    for(String id:ids) {
      assertNotNull("Id " + id + " not found in errors list", errors.get(id));
    }
  }
  
  protected SolrQueryResponse add(final String chain, SolrParams requestParams, final SolrInputDocument doc) throws IOException {
    return add(chain, requestParams, Arrays.asList(new SolrInputDocument[]{doc}));
  }
  
  protected SolrQueryResponse add(final String chain, SolrParams requestParams, final Collection<SolrInputDocument> docs) throws IOException {
    
    SolrCore core = h.getCore();
    UpdateRequestProcessorChain pc = core.getUpdateProcessingChain(chain);
    assertNotNull("No Chain named: " + chain, pc);
    
    SolrQueryResponse rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap<Object>());
    
    if(requestParams == null) {
      requestParams = new ModifiableSolrParams();
    }
    
    SolrQueryRequest req = new LocalSolrQueryRequest(core, requestParams);
    try {
      UpdateRequestProcessor processor = pc.createProcessor(req, rsp);
      for(SolrInputDocument doc:docs) {
        AddUpdateCommand cmd = new AddUpdateCommand(req);
        cmd.solrDoc = doc;
        processor.processAdd(cmd);
      }
      processor.finish();
      
    } finally {
      req.close();
    }
    return rsp;
  }
  
}
