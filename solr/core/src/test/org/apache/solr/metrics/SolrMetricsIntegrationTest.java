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

package org.apache.solr.metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.metrics.reporters.MockMetricReporter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SolrMetricsIntegrationTest extends SolrTestCaseJ4 {
  private static final int MAX_ITERATIONS = 20;
  private static final String METRIC_NAME = "requestTimes";
  private static final String HANDLER_NAME = "standard";
  private static final String[] REPORTER_NAMES = {"reporter1", "reporter2"};
  private static final SolrInfoMBean.Category HANDLER_CATEGORY = SolrInfoMBean.Category.QUERYHANDLER;

  @Before
  public void beforeTest() throws Exception {
    initCore("solrconfig-metricreporter.xml", "schema.xml");
  }

  @After
  public void afterTest() throws Exception {
    SolrCoreMetricManager metricManager = h.getCore().getMetricManager();
    Map<String, SolrMetricReporter> reporters = new HashMap<>(metricManager.getReporters());

    deleteCore();

    for (String reporterName : REPORTER_NAMES) {
      SolrMetricReporter reporter = reporters.get(reporterName);
      MockMetricReporter mockReporter = (MockMetricReporter) reporter;
      assertTrue("Reporter " + reporterName + " was not closed: " + mockReporter, mockReporter.didClose);
    }
  }

  @Test
  public void testConfigureReporter() throws Exception {
    Random random = random();

    int iterations = TestUtil.nextInt(random, 0, MAX_ITERATIONS);
    for (int i = 0; i < iterations; ++i) {
      h.query(req("*"));
    }

    String metricName = SolrMetricManager.mkName(METRIC_NAME, HANDLER_CATEGORY.toString(), HANDLER_NAME);
    SolrCoreMetricManager metricManager = h.getCore().getMetricManager();
    assertEquals(REPORTER_NAMES.length, metricManager.getReporters().size());

    for (String reporterName : REPORTER_NAMES) {
      SolrMetricReporter reporter = metricManager.getReporters().get(reporterName);
      assertNotNull("Reporter " + reporterName + " was not found.", reporter);
      assertTrue(reporter instanceof MockMetricReporter);

      MockMetricReporter mockReporter = (MockMetricReporter) reporter;
      assertTrue("Reporter " + reporterName + " was not initialized: " + mockReporter, mockReporter.didInit);
      assertTrue("Reporter " + reporterName + " was not validated: " + mockReporter, mockReporter.didValidate);
      assertFalse("Reporter " + reporterName + " was incorrectly closed: " + mockReporter, mockReporter.didClose);

      Metric metric = mockReporter.reportMetric(metricName);
      assertNotNull("Metric " + metricName + " was not reported.", metric);
      assertTrue("Metric " + metricName + " is not an instance of Timer: " + metric, metric instanceof Timer);

      Timer timer = (Timer) metric;
      assertEquals(iterations, timer.getCount());
    }
  }
}
