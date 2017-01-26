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
package org.apache.solr.metrics.reporters.solr;

import java.nio.file.Paths;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricReporter;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class SolrCloudReportersTest extends SolrCloudTestCase {

  @BeforeClass
  public static void configureDummyCluster() throws Exception {
    configureCluster(0).configure();
  }

  @Before
  public void closePreviousCluster() throws Exception {
    shutdownCluster();
  }

  @Test
  public void testExplicitConfiguration() throws Exception {
    String solrXml = IOUtils.toString(SolrCloudReportersTest.class.getResourceAsStream("/solr/solr-solrreporter.xml"), "UTF-8");
    configureCluster(2)
        .withSolrXml(solrXml).configure();
    cluster.uploadConfigSet(Paths.get(TEST_PATH().toString(), "configsets", "minimal", "conf"), "test");
    System.out.println("ZK: " + cluster.getZkServer().getZkAddress());
    CollectionAdminRequest.createCollection("test_collection", "test", 2, 2)
        .setMaxShardsPerNode(4)
        .process(cluster.getSolrClient());
    waitForState("Expected test_collection with 2 shards and 2 replicas", "test_collection", clusterShape(2, 2));
    cluster.getJettySolrRunners().forEach(jetty -> {
      CoreContainer cc = jetty.getCoreContainer();
      SolrMetricManager metricManager = cc.getMetricManager();
      Map<String, SolrMetricReporter> reporters = metricManager.getReporters("solr.overseer");
      assertEquals(reporters.toString(), 1, reporters.size());
      SolrMetricReporter reporter = reporters.get("test");
      assertNotNull(reporter);
      assertTrue(reporter.toString(), reporter instanceof SolrOverseerReporter);
      SolrOverseerReporter sor = (SolrOverseerReporter)reporter;
      assertEquals(111, sor.getPeriod());
      for (String registryName : metricManager.registryNames(".*\\.shard[0-9]\\.core.*")) {
        reporters = metricManager.getReporters(registryName);
        assertEquals(reporters.toString(), 1, reporters.size());
        reporter = reporters.get("test");
        assertNotNull(reporter);
        assertTrue(reporter.toString(), reporter instanceof SolrReplicaReporter);
        SolrReplicaReporter srr = (SolrReplicaReporter)reporter;
        assertEquals(111, srr.getPeriod());
      }
      for (String registryName : metricManager.registryNames(".*\\.leader")) {
        reporters = metricManager.getReporters(registryName);
        // no reporters registered for leader registry
        assertEquals(reporters.toString(), 0, reporters.size());
      }
    });
  }

  @Test
  public void testDefaultPlugins() throws Exception {
    String solrXml = IOUtils.toString(SolrCloudReportersTest.class.getResourceAsStream("/solr/solr.xml"), "UTF-8");
    configureCluster(2)
        .withSolrXml(solrXml).configure();
    cluster.uploadConfigSet(Paths.get(TEST_PATH().toString(), "configsets", "minimal", "conf"), "test");
    System.out.println("ZK: " + cluster.getZkServer().getZkAddress());
    CollectionAdminRequest.createCollection("test_collection", "test", 2, 2)
        .setMaxShardsPerNode(4)
        .process(cluster.getSolrClient());
    waitForState("Expected test_collection with 2 shards and 2 replicas", "test_collection", clusterShape(2, 2));
    cluster.getJettySolrRunners().forEach(jetty -> {
      CoreContainer cc = jetty.getCoreContainer();
      SolrMetricManager metricManager = cc.getMetricManager();
      Map<String, SolrMetricReporter> reporters = metricManager.getReporters("solr.overseer");
      assertEquals(reporters.toString(), 1, reporters.size());
      SolrMetricReporter reporter = reporters.get("overseerDefault");
      assertNotNull(reporter);
      assertTrue(reporter.toString(), reporter instanceof SolrOverseerReporter);
      SolrOverseerReporter sor = (SolrOverseerReporter)reporter;
      assertEquals(30, sor.getPeriod());
      for (String registryName : metricManager.registryNames(".*\\.shard[0-9]\\.core.*")) {
        reporters = metricManager.getReporters(registryName);
        assertEquals(reporters.toString(), 1, reporters.size());
        reporter = reporters.get("replicaDefault");
        assertNotNull(reporter);
        assertTrue(reporter.toString(), reporter instanceof SolrReplicaReporter);
        SolrReplicaReporter srr = (SolrReplicaReporter)reporter;
        assertEquals(30, srr.getPeriod());
      }
      for (String registryName : metricManager.registryNames(".*\\.leader")) {
        reporters = metricManager.getReporters(registryName);
        // no reporters registered for leader registry
        assertEquals(reporters.toString(), 0, reporters.size());
      }
    });
  }
}
