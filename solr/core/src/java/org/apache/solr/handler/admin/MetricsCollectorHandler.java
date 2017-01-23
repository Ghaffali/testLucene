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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;

import com.codahale.metrics.MetricRegistry;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.loader.ContentStreamLoader;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.loader.CSVLoader;
import org.apache.solr.handler.loader.JavabinLoader;
import org.apache.solr.handler.loader.JsonLoader;
import org.apache.solr.handler.loader.XMLLoader;
import org.apache.solr.metrics.AggregateMetric;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.reporters.solr.SolrReporter;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.MergeIndexesCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.util.stats.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler to collect and aggregate metric reports.
 */
public class MetricsCollectorHandler extends RequestHandlerBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String HANDLER_PATH = "/admin/metricsCollector";

  private final CoreContainer coreContainer;
  private final SolrMetricManager metricManager;
  private final Map<String, ContentStreamLoader> registry = new HashMap<>();
  private SolrParams params;

  public MetricsCollectorHandler(final CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
    this.metricManager = coreContainer.getMetricManager();

  }

  @Override
  public void init(NamedList initArgs) {
    super.init(initArgs);
    if (initArgs != null) {
      params = SolrParams.toSolrParams(initArgs);
    } else {
      params = new ModifiableSolrParams();
    }
    registry.put("application/xml", new XMLLoader().init(params) );
    registry.put("application/json", new JsonLoader().init(params) );
    registry.put("application/csv", new CSVLoader().init(params) );
    registry.put("application/javabin", new JavabinLoader().init(params) );
    registry.put("text/csv", registry.get("application/csv") );
    registry.put("text/xml", registry.get("application/xml") );
    registry.put("text/json", registry.get("application/json"));
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    //log.info("#### " + req.toString());
    for (ContentStream cs : req.getContentStreams()) {
      if (cs.getContentType() == null) {
        log.warn("Missing content type - ignoring");
        continue;
      }
      ContentStreamLoader loader = registry.get(cs.getContentType());
      if (loader == null) {
        throw new SolrException(SolrException.ErrorCode.UNSUPPORTED_MEDIA_TYPE, "Unsupported content type for stream: " + cs.getSourceInfo() + ", contentType=" + cs.getContentType());
      }
      String reporterId = req.getParams().get(SolrReporter.REPORTER_ID);
      String group = req.getParams().get(SolrReporter.GROUP_ID);
      if (reporterId == null || group == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing " + SolrReporter.REPORTER_ID +
            " or " + SolrReporter.GROUP_ID + " in request params: " + req.getParamString());
      }
      MetricRegistry registry = metricManager.registry(group);
      loader.load(req, rsp, cs, new MetricUpdateProcessor(registry, reporterId, group));
    }
  }

  @Override
  public String getDescription() {
    return "Handler for collecting and aggregating metric reports.";
  }

  private static class MetricUpdateProcessor extends UpdateRequestProcessor {
    private final MetricRegistry registry;
    private final String reporterId;
    private final String group;

    public MetricUpdateProcessor(MetricRegistry registry, String reporterId, String group) {
      super(null);
      this.registry = registry;
      this.reporterId = reporterId;
      this.group = group;
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
      SolrInputDocument doc = cmd.solrDoc;
      if (doc == null) {
        return;
      }
      String metricName = (String)doc.getFieldValue(MetricUtils.NAME);
      if (metricName == null) {
        log.warn("Missing metric 'name' field in document, skipping: " + doc);
        return;
      }
      doc.remove(MetricUtils.NAME);
      // already known
      doc.remove(SolrReporter.REPORTER_ID);
      doc.remove(SolrReporter.GROUP_ID);
      // remaining fields should only contain numeric values
      doc.forEach(f -> {
        if (f.getFirstValue() instanceof Number) {
          String key = MetricRegistry.name(metricName, f.getName());
          AggregateMetric metric = (AggregateMetric)registry.getMetrics().get(key);
          if (metric == null) {
            metric = new AggregateMetric();
            registry.register(key, metric);
          }
          Object o = f.getFirstValue();
          if (o != null && (o instanceof Number)) {
            metric.set(reporterId, ((Number)o).doubleValue());
          } else {
            // silently discard
          }
        } else {
          // silently discard
        }
      });
    }

    @Override
    public void processDelete(DeleteUpdateCommand cmd) throws IOException {
      super.processDelete(cmd);
    }

    @Override
    public void processMergeIndexes(MergeIndexesCommand cmd) throws IOException {
      throw new UnsupportedOperationException("processMergeIndexes");
    }

    @Override
    public void processCommit(CommitUpdateCommand cmd) throws IOException {
      throw new UnsupportedOperationException("processCommit");
    }

    @Override
    public void processRollback(RollbackUpdateCommand cmd) throws IOException {
      throw new UnsupportedOperationException("processRollback");
    }
  }
}
