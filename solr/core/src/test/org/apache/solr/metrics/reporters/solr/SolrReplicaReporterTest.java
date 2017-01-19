package org.apache.solr.metrics.reporters.solr;

import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.junit.Test;

/**
 *
 */
public class SolrReplicaReporterTest extends AbstractFullDistribZkTestBase {
  public SolrReplicaReporterTest() {
    schemaString = "schema15.xml";      // we need a string id
  }

  @Test
  public void test() throws Exception {
    printLayout();
    Thread.sleep(10000000);
  }
}
