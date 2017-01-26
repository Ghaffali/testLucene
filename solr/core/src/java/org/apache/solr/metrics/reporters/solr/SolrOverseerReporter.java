package org.apache.solr.metrics.reporters.solr;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.http.client.HttpClient;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.handler.admin.MetricsCollectorHandler;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricReporter;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SolrOverseerReporter extends SolrMetricReporter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String OVERSEER_GROUP = SolrMetricManager.overridableRegistryName(SolrInfoMBean.Group.overseer.toString());

  public static final List<SolrReporter.Specification> DEFAULT_METRICS = new ArrayList<SolrReporter.Specification>() {{
    add(new SolrReporter.Specification(OVERSEER_GROUP, "jetty",
        SolrMetricManager.overridableRegistryName(SolrInfoMBean.Group.jetty.toString()),
        Collections.singleton(".*\\.mean"))); // all metrics
    add(new SolrReporter.Specification(OVERSEER_GROUP, "jvm",
        SolrMetricManager.overridableRegistryName(SolrInfoMBean.Group.jvm.toString()),
        Collections.emptySet())); // all metrics
    add(new SolrReporter.Specification(OVERSEER_GROUP, "node", SolrMetricManager.overridableRegistryName(SolrInfoMBean.Group.node.toString()),
        Collections.emptySet())); // all metrics
    add(new SolrReporter.Specification(OVERSEER_GROUP, "leader.$1", "solr\\.core\\.(.*)\\.leader",
        new HashSet<String>(){{
          add("UPDATE\\./update/.*");
          add("QUERY\\./select.*");
          add("INDEX\\..*");
          add("TLOG\\..*");
    }}));
  }};

  private String handler = MetricsCollectorHandler.HANDLER_PATH;
  private int period = 60;
  private List<SolrReporter.Specification> metrics = DEFAULT_METRICS;

  private SolrReporter reporter;

  /**
   * Create a reporter for metrics managed in a named registry.
   *
   * @param metricManager metric manager
   * @param registryName  unlike in other reporters, this is the node id
   */
  public SolrOverseerReporter(SolrMetricManager metricManager, String registryName) {
    super(metricManager, registryName);
  }

  public void setHandler(String handler) {
    this.handler = handler;
  }

  public void setPeriod(int period) {
    this.period = period;
  }

  // for unit tests
  public int getPeriod() {
    return period;
  }

  @Override
  protected void validate() throws IllegalStateException {
    if (period < 1) {
      log.info("Turning off node reporter, period=" + period);
    }
    // start in setCoreContainer(...)
  }

  @Override
  public void close() throws IOException {
    if (reporter != null) {
      reporter.close();;
    }
  }

  public void setCoreContainer(CoreContainer cc) {
    // start reporter only in cloud mode
    if (!cc.isZooKeeperAware()) {
      return;
    }
    if (period < 1) { // don't start it
      return;
    }
    HttpClient httpClient = cc.getUpdateShardHandler().getHttpClient();
    ZkController zk = cc.getZkController();
    String reporterId = zk.getNodeName();
    reporter = SolrReporter.Builder.forRegistries(metricManager, metrics)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .withHandler(handler)
        .withReporterId(reporterId)
        .cloudClient(false) // we want to send reports specifically to a selected leader instance
        .skipAggregateValues(true) // we don't want to transport details of aggregates
        .skipHistograms(true) // we don't want to transport histograms
        .build(httpClient, new OverseerUrlSupplier(zk));

    reporter.start(period, TimeUnit.SECONDS);

  }

  // TODO: fix this when there is an elegant way to retrieve URL of a node that runs Overseer leader.
  private static class OverseerUrlSupplier implements Supplier<String> {
    private static final long DEFAULT_INTERVAL = 30000; // 30s
    private ZkController zk;
    private String lastKnownUrl = null;
    private long lastCheckTime = 0;
    private long interval = DEFAULT_INTERVAL;

    OverseerUrlSupplier(ZkController zk) {
      this.zk = zk;
    }

    @Override
    public String get() {
      if (zk == null) {
        return null;
      }
      // primitive caching for interval
      long now = System.currentTimeMillis();
      if (lastKnownUrl != null && (now - lastCheckTime) < interval) {
        return lastKnownUrl;
      }
      if (!zk.isConnected()) {
        return lastKnownUrl;
      }
      lastCheckTime = now;
      SolrZkClient zkClient = zk.getZkClient();
      ZkNodeProps props;
      try {
        props = ZkNodeProps.load(zkClient.getData(
            Overseer.OVERSEER_ELECT + "/leader", null, null, true));
      } catch (KeeperException e) {
        log.warn("Could not obtain overseer's address, skipping.", e);
        return lastKnownUrl;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return lastKnownUrl;
      }
      if (props == null) {
        return lastKnownUrl;
      }
      String oid = props.getStr("id");
      if (oid == null) {
        return lastKnownUrl;
      }
      String[] ids = oid.split("-");
      if (ids.length != 3) { // unknown format
        log.warn("Unknown format of leader id, skipping: " + oid);
        return lastKnownUrl;
      }
      // convert nodeName back to URL
      String url = zk.getZkStateReader().getBaseUrlForNodeName(ids[1]);
      // check that it's parseable
      try {
        new java.net.URL(url);
      } catch (MalformedURLException mue) {
        log.warn("Malformed Overseer's leader URL: url", mue);
        return lastKnownUrl;
      }
      lastKnownUrl = url;
      return url;
    }
  }

}
