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

import java.util.List;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.handler.component.RealTimeGetComponent;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.internal.matchers.StringContains.containsString;

public class UpdateLogTest extends SolrTestCaseJ4 {

  static UpdateLog ulog = null;

  @BeforeClass
  public static void beforeClass() throws Exception {

    // nocommit: does this test need to randomize between diff schema/fields used?
    // nocommit: see nocommits/jira questions related to special dynamicField logic in AtomicUpdateDocumentMerger.isInPlaceUpdate
    
    initCore("solrconfig-tlog.xml", "schema-inplace-updates.xml");

    try (SolrQueryRequest req = req()) {
      UpdateHandler uhandler = req.getCore().getUpdateHandler();
      ((DirectUpdateHandler2) uhandler).getCommitTracker().setTimeUpperBound(100);
      ((DirectUpdateHandler2) uhandler).getCommitTracker().setOpenSearcher(false);
      ulog = uhandler.getUpdateLog();
    }
  }

  @Test
  /**
   * @see org.apache.solr.update.UpdateLog#applyPartialUpdates(BytesRef,long,long,SolrDocumentBase)
   */
  public void testApplyPartialUpdatesOnMultipleInPlaceUpdatesInSequence() {    
    // Add a full update, two in-place updates and verify applying partial updates is working
    AddUpdateCommand cmd;
    cmd = getAddUpdate(null, sdoc("id", "1", "title_s", "title1", "val1_i_dvo", "1", "_version_", "100"));
    ulog.add(cmd);
    ulog.add(getAddUpdate(100L, sdoc("id", "1", "price", "1000", "val1_i_dvo", "2", "_version_", "101")));
    ulog.add(getAddUpdate(101L, sdoc("id", "1", "val1_i_dvo", "3", "_version_", "102")));

    Object partialUpdate = ulog.lookup(cmd.getIndexedId());
    SolrDocument partialDoc = RealTimeGetComponent.toSolrDoc((SolrInputDocument)((List)partialUpdate).get(4), 
        h.getCore().getLatestSchema(), true);
    long prevVersion = (Long)((List)partialUpdate).get(3);
    long prevPointer = (Long)((List)partialUpdate).get(2);

    assertEquals(3L, ((NumericDocValuesField)partialDoc.getFieldValue("val1_i_dvo")).numericValue());
    assertFalse(partialDoc.containsKey("title_s"));

    long returnVal = ulog.applyPartialUpdates(cmd.getIndexedId(), prevPointer, prevVersion, partialDoc);

    assertEquals(0, returnVal);
    assertEquals(1000, Integer.parseInt(partialDoc.getFieldValue("price").toString()));
    assertEquals(3L, ((NumericDocValuesField)partialDoc.getFieldValue("val1_i_dvo")).numericValue());
    assertEquals("title1", partialDoc.getFieldValue("title_s"));

    // Add a full update, commit, then two in-place updates, and verify that applying partial updates is working (since
    // the prevTlog and prevTlog2 are retained after a commit
    ulogCommit(ulog);
    if (random().nextBoolean()) { // sometimes also try a second commit
      ulogCommit(ulog);
    }
    ulog.add(getAddUpdate(102L, sdoc("id", "1", "price", "2000", "val1_i_dvo", "4", "_version_", "200")));    
    ulog.add(getAddUpdate(200L, sdoc("id", "1", "val1_i_dvo", "5", "_version_", "201")));

    partialUpdate = ulog.lookup(cmd.getIndexedId());
    partialDoc = RealTimeGetComponent.toSolrDoc((SolrInputDocument)((List)partialUpdate).get(4), h.getCore().getLatestSchema(), true);
    prevVersion = (Long)((List)partialUpdate).get(3);
    prevPointer = (Long)((List)partialUpdate).get(2);

    assertEquals(5L, ((NumericDocValuesField)partialDoc.getFieldValue("val1_i_dvo")).numericValue());
    assertFalse(partialDoc.containsKey("title_s"));

    returnVal = ulog.applyPartialUpdates(cmd.getIndexedId(), prevPointer, prevVersion, partialDoc);

    assertEquals(0, returnVal);
    assertEquals(2000, Integer.parseInt(partialDoc.getFieldValue("price").toString()));
    assertEquals(5L, ((NumericDocValuesField)partialDoc.getFieldValue("val1_i_dvo")).numericValue());
    assertEquals("title1", partialDoc.getFieldValue("title_s"));
  }
  
  @Test
  public void testApplyPartialUpdatesAfterMultipleCommits() {    
    AddUpdateCommand cmd;
    cmd = getAddUpdate(null, sdoc("id", "1", "title_s", "title1", "val1_i_dvo", "1", "_version_", "100"));
    ulog.add(cmd);
    ulog.add(getAddUpdate(100L, sdoc("id", "1", "price", "1000", "val1_i_dvo", "2", "_version_", "101")));
    ulog.add(getAddUpdate(101L, sdoc("id", "1", "val1_i_dvo", "3", "_version_", "102")));

    // Do 3 commits, then in-place update, and verify that applying partial updates can't find full doc
    for (int i=0; i<3; i++)
      ulogCommit(ulog);
    ulog.add(getAddUpdate(101L, sdoc("id", "1", "val1_i_dvo", "6", "_version_", "300")));

    Object partialUpdate = ulog.lookup(cmd.getIndexedId());
    SolrDocument partialDoc = RealTimeGetComponent.toSolrDoc((SolrInputDocument)((List)partialUpdate).get(4), h.getCore().getLatestSchema(), true);
    long prevVersion = (Long)((List)partialUpdate).get(3);
    long prevPointer = (Long)((List)partialUpdate).get(2);

    assertEquals(6L, ((NumericDocValuesField)partialDoc.getFieldValue("val1_i_dvo")).numericValue());
    assertFalse(partialDoc.containsKey("title_s"));

    long returnVal = ulog.applyPartialUpdates(cmd.getIndexedId(), prevPointer, prevVersion, partialDoc);

    assertEquals(-1, returnVal);
  }

  @Test
  public void testApplyPartialUpdatesDependingOnNonAddShouldThrowException() {
    AddUpdateCommand cmd;
    cmd = getAddUpdate(null, sdoc("id", "1", "title_s", "title1", "val1_i_dvo", "1", "_version_", "100"));
    ulog.add(cmd);

    ulog.delete(getDeleteUpdate("1", 500L, false)); // dbi
    ulog.add(getAddUpdate(500L, sdoc("id", "1", "val1_i_dvo", "2", "_version_", "501")));
    ulog.add(getAddUpdate(501L, sdoc("id", "1", "val1_i_dvo", "3", "_version_", "502")));

    Object partialUpdate = ulog.lookup(cmd.getIndexedId());
    SolrDocument partialDoc = RealTimeGetComponent.toSolrDoc((SolrInputDocument)((List)partialUpdate).get(4), h.getCore().getLatestSchema(), true);
    long prevVersion = (Long)((List)partialUpdate).get(3);
    long prevPointer = (Long)((List)partialUpdate).get(2);

    assertEquals(3L, ((NumericDocValuesField)partialDoc.getFieldValue("val1_i_dvo")).numericValue());
    assertEquals(502L, ((NumericDocValuesField)partialDoc.getFieldValue("_version_")).numericValue());
    assertFalse(partialDoc.containsKey("title_s"));

    // If an in-place update depends on a non-add (i.e. DBI), assert that an exception is thrown.
    SolrException ex = expectThrows(SolrException.class, () -> {
        long returnVal = ulog.applyPartialUpdates(cmd.getIndexedId(), prevPointer, prevVersion, partialDoc);
        fail("502 depends on 501, 501 depends on 500, but 500 is a"
             + " DELETE. This should've generated an exception. returnVal is: "+returnVal);
      });
    assertEquals(ex.toString(), SolrException.ErrorCode.INVALID_STATE.code, ex.code());
    assertThat(ex.getMessage(), containsString("should've been either ADD or UPDATE_INPLACE"));
    assertThat(ex.getMessage(), containsString("looking for id=1"));
  }

  @Test
  public void testApplyPartialUpdatesWithDBQ() { // nocommit: missleading name?

    // nocommit: no in-place updates happening in this test?
    
    AddUpdateCommand cmd;
    cmd = getAddUpdate(null, sdoc("id", "1", "title_s", "title1", "val1_i_dvo", "1", "_version_", "100"));
    ulog.add(cmd);
    ulog.add(getAddUpdate(100L, sdoc("id", "1", "val1_i_dvo", "2", "_version_", "101")));
    ulog.add(getAddUpdate(101L, sdoc("id", "1", "val1_i_dvo", "3", "_version_", "102")));
    ulog.deleteByQuery(getDeleteUpdate("1", 200L, true)); // dbq, "id:1"
    assertNull(ulog.lookup(cmd.getIndexedId()));

    // nocommit: need more rigerous assertions about expected behavior after DBQ (new RT searcher?)
  }

  /**
   * Simulate a commit at a given updateLog
   */
  private static void ulogCommit(UpdateLog ulog) {
    try (SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), params())) {
      CommitUpdateCommand commitCmd = new CommitUpdateCommand(req, false);
      ulog.preCommit(commitCmd);
      ulog.postCommit(commitCmd);
    }
  }

  /**
   * Obtain a DeleteUpdateCommand for a deleteById operation
   */
  private static DeleteUpdateCommand getDeleteUpdate(String id, long version, boolean dbq) {
    // nocommit: req lifecycle bug
    // nocommit: cmd returned is linked to req that's already been closed
    // nocommit: see jira comments for suggested fix
    try (SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), params())) {
      DeleteUpdateCommand cmd = new DeleteUpdateCommand(req);
      if (dbq) {
        cmd.query = ("id:"+id);
      } else {
        cmd.id = id;
      }
      cmd.setVersion(version);
      return cmd;
    }
  }

  /**
   *   Obtain an AddUpdateCommand for a full add/in-place update operation.
   *   If there's a non-null prevVersion, then this AddUpdateCommand represents an in-place update.
   *   This method, when prevVersion is passed in (i.e. for in-place update), represents an 
   *   AddUpdateCommand that has undergone the merge process and inc/set operations have now been
   *   converted into actual values that just need to be written. 
   */
  public static AddUpdateCommand getAddUpdate(Long prevVersion, SolrInputDocument sdoc) {
    // nocommit: req lifecycle bug
    // nocommit: cmd returned is linked to req that's already been closed
    // nocommit: see jira comments for suggested fix
    AddUpdateCommand cmd; 
    try (SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), params())) {
      cmd = new AddUpdateCommand(req);
    }
    cmd.solrDoc = sdoc;
    assertTrue(cmd.solrDoc.containsKey(DistributedUpdateProcessor.VERSION_FIELD));
    cmd.setVersion(Long.parseLong(cmd.solrDoc.getFieldValue(DistributedUpdateProcessor.VERSION_FIELD).toString()));
    if (prevVersion != null) {
      cmd.prevVersion = prevVersion;
    }
    return cmd;
  }

}
