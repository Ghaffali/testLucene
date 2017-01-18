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
package org.apache.solr.handler.admin;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import com.codahale.metrics.MetricRegistry;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.metrics.AggregateMetric;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class MetricsCollectorHandler extends RequestHandlerBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String HANDLER_PATH = "/admin/metricsCollector";

  private final CoreContainer coreContainer;
  private final SolrMetricManager metricManager;

  public MetricsCollectorHandler(final CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
    this.metricManager = coreContainer.getMetricManager();
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    log.info("#### " + req.toString());
    for (ContentStream cs : req.getContentStreams()) {
      // only javabin supported
      if (!cs.getContentType().equals("application/javabin")) {
        log.warn("Invalid content type, skipping " + cs.getSourceInfo() + ": " + cs.getContentType());
      }
      JavaBinCodec codec = new JavaBinCodec();
      NamedList nl = (NamedList)codec.unmarshal(cs.getStream());
      String id = (String)nl.get("id");
      String group = (String)nl.get("group");
      MetricRegistry registry = metricManager.registry(group);
      // values are tuples of String / NamedList<Number>
      NamedList values = (NamedList)nl.get("values");
      values.forEach((k, v) -> {
        ((NamedList)v).forEach((k1, v1) -> {
          String key = MetricRegistry.name(k.toString(), k1.toString());
          AggregateMetric metric = (AggregateMetric)registry.getMetrics().get(key);
          if (metric == null) {
            metric = new AggregateMetric();
            registry.register(key, metric);
          }
          metric.set(id, ((Number)v1).doubleValue());
        });
      });
    }
  }

  @Override
  public String getDescription() {
    return null;
  }
}
