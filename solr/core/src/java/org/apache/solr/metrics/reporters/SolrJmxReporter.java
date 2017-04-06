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
package org.apache.solr.metrics.reporters;

import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricRegistryListener;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricReporter;
import org.apache.solr.util.JmxUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SolrMetricReporter} that finds (or creates) a MBeanServer from
 * the given configuration and registers metrics to it with JMX.
 * <p>NOTE: {@link JmxReporter} that this class uses exports only newly added metrics (it doesn't
 * process already existing metrics in a registry)</p>
 */
public class SolrJmxReporter extends SolrMetricReporter {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private String domain;
  private String agentId;
  private String serviceUrl;

  private JmxReporter reporter;
  private MetricRegistry registry;
  private MBeanServer mBeanServer;
  private MetricsMapListener listener;
  private boolean enabled = true;

  /**
   * Creates a new instance of {@link SolrJmxReporter}.
   *
   * @param registryName name of the registry to report
   */
  public SolrJmxReporter(SolrMetricManager metricManager, String registryName) {
    super(metricManager, registryName);
    setDomain(registryName);
  }

  /**
   * Initializes the reporter by finding an MBeanServer
   * and registering the metricManager's metric registry.
   *
   * @param pluginInfo the configuration for the reporter
   */
  @Override
  public synchronized void init(PluginInfo pluginInfo) {
    super.init(pluginInfo);
    if (!enabled) {
      log.info("JMX monitoring disabled for registry " + registryName);
      return;
    }
    log.info("Initializing for registry " + registryName);
    if (serviceUrl != null && agentId != null) {
      mBeanServer = JmxUtil.findFirstMBeanServer();
      log.warn("No more than one of serviceUrl(%s) and agentId(%s) should be configured, using first MBeanServer instead of configuration.",
          serviceUrl, agentId, mBeanServer);
    } else if (serviceUrl != null) {
      try {
        mBeanServer = JmxUtil.findMBeanServerForServiceUrl(serviceUrl);
      } catch (IOException e) {
        log.warn("findMBeanServerForServiceUrl(%s) exception: %s", serviceUrl, e);
        mBeanServer = null;
      }
    } else if (agentId != null) {
      mBeanServer = JmxUtil.findMBeanServerForAgentId(agentId);
    } else {
      mBeanServer = JmxUtil.findFirstMBeanServer();
      log.debug("No serviceUrl or agentId was configured, using first MBeanServer: " + mBeanServer);
    }

    if (mBeanServer == null) {
      log.warn("No JMX server found. Not exposing Solr metrics via JMX.");
      return;
    }

    JmxObjectNameFactory jmxObjectNameFactory = new JmxObjectNameFactory(pluginInfo.name, domain);
    registry = metricManager.registry(registryName);
    // filter out MetricsMap gauges - we have a better way of handling them
    MetricFilter filter = (name, metric) -> !(metric instanceof MetricsMap);

    reporter = JmxReporter.forRegistry(registry)
                          .registerWith(mBeanServer)
                          .inDomain(domain)
                          .filter(filter)
                          .createsObjectNamesWith(jmxObjectNameFactory)
                          .build();
    reporter.start();
    // workaround for inability to register custom MBeans (to be available in metrics 4.0?)
    listener = new MetricsMapListener(mBeanServer, jmxObjectNameFactory);
    registry.addListener(listener);

    log.info("JMX monitoring for registry '" + registryName + "' enabled at server: " + mBeanServer);
  }

  /**
   * Stops the reporter from publishing metrics.
   */
  @Override
  public synchronized void close() {
    if (reporter != null) {
      reporter.close();
      reporter = null;
    }
    if (listener != null && registry != null) {
      registry.removeListener(listener);
      listener.close();
      listener = null;
    }
  }

  /**
   * Validates that the reporter has been correctly configured.
   * Note that all configurable arguments are currently optional.
   *
   * @throws IllegalStateException if the reporter is not properly configured
   */
  @Override
  protected void validate() throws IllegalStateException {
    // Nothing to validate
  }

  /**
   * Sets the domain with which MBeans are published. If none is set,
   * the domain defaults to the name of the core.
   *
   * @param domain the domain
   */
  public void setDomain(String domain) {
    if (domain != null) {
      this.domain = domain;
    } else {
      this.domain = registryName;
    }
  }

  /**
   * Sets the service url for a JMX server.
   * Note that this configuration is optional.
   *
   * @param serviceUrl the service url
   */
  public void setServiceUrl(String serviceUrl) {
    this.serviceUrl = serviceUrl;
  }

  /**
   * Sets the agent id for a JMX server.
   * Note that this configuration is optional.
   *
   * @param agentId the agent id
   */
  public void setAgentId(String agentId) {
    this.agentId = agentId;
  }

  /**
   * Return configured agentId or null.
   */
  public String getAgentId() {
    return agentId;
  }

  /**
   * Return configured serviceUrl or null.
   */
  public String getServiceUrl() {
    return serviceUrl;
  }

  /**
   * Return configured domain or null.
   */
  public String getDomain() {
    return domain;
  }

  /**
   * Enable reporting, defaults to true.
   * @param enabled enable, defaults to true when null or not set.
   */
  public void setEnabled(Boolean enabled) {
    if (enabled == null) {
      this.enabled = true;
    } else {
      this.enabled = enabled;
    }
  }

  /**
   * Return the reporter's MBeanServer.
   *
   * @return the reporter's MBeanServer
   */
  public MBeanServer getMBeanServer() {
    return mBeanServer;
  }

  /**
   * For unit tests.
   * @return true if this reporter is actively reporting metrics to JMX.
   */
  public boolean isActive() {
    return reporter != null;
  }

  @Override
  public String toString() {
    return String.format(Locale.ENGLISH, "[%s@%s: domain = %s, service url = %s, agent id = %s]",
        getClass().getName(), Integer.toHexString(hashCode()), domain, serviceUrl, agentId);
  }

  private static class MetricsMapListener extends MetricRegistryListener.Base {
    MBeanServer server;
    JmxObjectNameFactory nameFactory;
    // keep the names so that we can unregister them on core close
    Set<ObjectName> registered = new HashSet<>();
    // prevent ConcurrentModificationException when closing
    ReentrantLock lock = new ReentrantLock();

    MetricsMapListener(MBeanServer server, JmxObjectNameFactory nameFactory) {
      this.server = server;
      this.nameFactory = nameFactory;
    }

    @Override
    public void onGaugeAdded(String name, Gauge<?> gauge) {
      if (!(gauge instanceof MetricsMap)) {
        return;
      }
      if (!lock.tryLock()) {
        return;
      }
      try {
        ObjectName objectName = nameFactory.createName("gauges", nameFactory.getDomain(), name);
        if (server.isRegistered(objectName)) {
          // silently unregister - may have been left over from a previous reporter
          server.unregisterMBean(objectName);
        }
        // some MBean servers re-write object name to include additional properties
        ObjectInstance instance = server.registerMBean(gauge, objectName);
        if (instance != null) {
          registered.add(instance.getObjectName());
        }
      } catch (Exception e) {
        log.warn("bean registration error", e);
      } finally {
        lock.unlock();
      }
    }

    public void close() {
      lock.lock();
      try {
        for (ObjectName name : registered) {
          try {
            if (server.isRegistered(name)) {
              server.unregisterMBean(name);
            }
          } catch (Exception e) {
            log.warn("bean unregistration error", e);
          }
        }
        registered.clear();
      } finally {
        lock.unlock();
      }
    }
  }
}
