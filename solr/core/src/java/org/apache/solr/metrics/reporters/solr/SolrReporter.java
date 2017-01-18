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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.admin.MetricsCollectorHandler;
import org.apache.solr.util.stats.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SolrReporter extends ScheduledReporter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static class Builder {
    private final MetricRegistry registry;
    private String id;
    private String group;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private MetricFilter filter;
    private String handler;
    private boolean skipHistograms;

    public static Builder forRegistry(MetricRegistry registry) {
      return new Builder(registry);
    }

    private Builder(MetricRegistry registry) {
      this.registry = registry;
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.filter = MetricFilter.ALL;
      this.skipHistograms = false;
    }

    /**
     * Histograms are difficult / impossible to aggregate, so it may not be
     * worth to report them.
     * @param skipHistograms
     * @return {@code this}
     */
    public Builder skipHistograms(boolean skipHistograms) {
      this.skipHistograms = skipHistograms;
      return this;
    }

    /**
     * Handler name to use at the remote end.
     *
     * @param handler handler name, eg. "/admin/metricsCollector"
     * @return {@code this}
     */
    public Builder withHandler(String handler) {
      this.handler = handler;
      return this;
    }

    /**
     * Use this id to identify metrics from this instance.
     *
     * @param id
     * @return {@code this}
     */
    public Builder withId(String id) {
      this.id = id;
      return this;
    }

    /**
     * Use this id to identify a logical group of reports.
     *
     * @param group
     * @return {@code this}
     */
    public Builder withGroup(String group) {
      this.group = group;
      return this;
    }

    /**
     * Convert rates to the given time unit.
     *
     * @param rateUnit a unit of time
     * @return {@code this}
     */
    public Builder convertRatesTo(TimeUnit rateUnit) {
      this.rateUnit = rateUnit;
      return this;
    }

    /**
     * Convert durations to the given time unit.
     *
     * @param durationUnit a unit of time
     * @return {@code this}
     */
    public Builder convertDurationsTo(TimeUnit durationUnit) {
      this.durationUnit = durationUnit;
      return this;
    }

    /**
     * Only report metrics which match the given filter.
     *
     * @param filter a {@link MetricFilter}
     * @return {@code this}
     */
    public Builder filter(MetricFilter filter) {
      this.filter = filter;
      return this;
    }

    /**
     * Build it.
     * @param client an instance of {@link HttpClient} to be used for making calls.
     * @param urlProvider function that returns the base URL of Solr instance to target. May return
     *                    null to indicate that reporting should be skipped. Note: this
     *                    function will be called every time just before report is sent.
     * @return configured instance of reporter
     */
    public SolrReporter build(HttpClient client, Supplier<String> urlProvider) {
      return new SolrReporter(client, urlProvider, registry, handler, id, group, rateUnit, durationUnit, filter, skipHistograms);
    }

  }

  private String id;
  private String group;
  private String handler;
  private Supplier<String> urlProvider;
  private SolrClientCache clientCache;
  private List<MetricFilter> filters;
  private MetricRegistry visibleRegistry;
  private boolean skipHistograms;

  public SolrReporter(HttpClient httpClient, Supplier<String> urlProvider, MetricRegistry registry, String handler,
                      String id, String group, TimeUnit rateUnit, TimeUnit durationUnit, MetricFilter filter,
                      boolean skipHistograms) {
    super(registry, "solr-reporter", filter, rateUnit, durationUnit);
    this.urlProvider = urlProvider;
    this.id = id;
    this.group = group;
    if (handler == null) {
      handler = MetricsCollectorHandler.HANDLER_PATH;
    }
    this.handler = handler;
    this.clientCache = new SolrClientCache(httpClient);
    // the one in superclass is invisible... :(
    this.visibleRegistry = registry;
    if (filter == null) {
      filter = MetricFilter.ALL;
    }
    this.filters = Collections.singletonList(filter);
    this.skipHistograms = skipHistograms;
  }

  @Override
  public void report() {
    String url = urlProvider.get();
    // if null then suppress reporting
    if (url == null) {
      return;
    }
    NamedList nl = MetricUtils.toNamedList(visibleRegistry, filters, MetricFilter.ALL, skipHistograms);
    NamedList report = new NamedList();
    report.add("id", id);
    report.add("group", group);
    report.add("values", nl);
    JavaBinCodec codec = new JavaBinCodec();
    SolrClient solr = clientCache.getHttpSolrClient(url);
    MetricsReportRequest req = new MetricsReportRequest(handler, null, report);
    try {
      solr.request(req);
    } catch (SolrServerException sse) {
      log.warn("Error sending metric report", sse);
    } catch (IOException ioe) {
      log.warn("Error sending metric report", ioe);
    }

  }

  @Override
  public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
    // no-op - we do all the work in report()
  }
}