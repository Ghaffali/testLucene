package org.apache.solr.metrics.reporters;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class MetricServiceRegistry<T> {
  protected Map<String, T> registry = new ConcurrentHashMap<>();

  public void register(String id, T service) {
    registry.put(id, service);
  }

  public T get(String id) {
    return registry.get(id);
  }

  public T unregister(String id) {
    return registry.remove(id);
  }
}
