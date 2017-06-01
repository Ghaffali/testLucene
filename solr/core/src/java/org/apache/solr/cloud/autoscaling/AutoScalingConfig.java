package org.apache.solr.cloud.autoscaling;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.solr.common.params.AutoScalingParams;
import org.apache.solr.common.util.Utils;

/**
 * Simple bean representation of <code>autoscaling.json</code>, which parses data
 * lazily.
 */
public class AutoScalingConfig {

  private final Map<String, Object> jsonMap;

  private Policy policy;
  private Map<String, TriggerConfig> triggers;
  private Map<String, ListenerConfig> listeners;

  /**
   * Bean representation of {@link org.apache.solr.cloud.autoscaling.AutoScaling.TriggerListener} config.
   */
  public static class ListenerConfig {
    public String trigger;
    public List<String> stages;
    public String listenerClass;
    public List<Map<String, String>> beforeActions;
    public List<Map<String, String>> afterActions;

    public ListenerConfig(Map<String, Object> properties) {
      trigger = (String)properties.get(AutoScalingParams.TRIGGER);
      stages = (List<String>)properties.getOrDefault(AutoScalingParams.STAGE, Collections.emptyList());
      listenerClass = (String)properties.get(AutoScalingParams.CLASS);
      beforeActions = (List<Map<String, String>>)properties.getOrDefault(AutoScalingParams.BEFORE_ACTION, Collections.emptyList());
      afterActions = (List<Map<String, String>>)properties.getOrDefault(AutoScalingParams.AFTER_ACTION, Collections.emptyList());
    }
  }

  /**
   * Bean representation of {@link org.apache.solr.cloud.autoscaling.AutoScaling.Trigger} config.
   */
  public static class TriggerConfig {
    public final AutoScaling.EventType eventType;
    public final Map<String, Object> properties = new HashMap<>();

    public TriggerConfig(Map<String, Object> properties) {
      String event = (String) properties.get(AutoScalingParams.EVENT);
      this.eventType = AutoScaling.EventType.valueOf(event.toUpperCase(Locale.ROOT));
      this.properties.putAll(properties);
    }
  }

  public AutoScalingConfig(Map<String, Object> jsonMap) {
    this.jsonMap = Utils.getDeepCopy(jsonMap, 10);
  }

  public Policy getPolicy() {
    if (policy == null) {
      policy = new Policy(jsonMap);
    }
    return policy;
  }

  public Map<String, TriggerConfig> getTriggerConfigs() {
    if (triggers == null) {
      Map<String, Object> trigMap = (Map<String, Object>)jsonMap.get("triggers");
      if (trigMap == null) {
        triggers = Collections.emptyMap();
      } else {
        triggers = new HashMap<>(trigMap.size());
        for (Map.Entry<String, Object> entry : trigMap.entrySet()) {
          triggers.put(entry.getKey(), new TriggerConfig((Map<String, Object>)entry.getValue()));
        }
      }
    }
    return triggers;
  }

  public Map<String, ListenerConfig> getListenerConfigs() {
    if (listeners == null) {
      Map<String, Object> map = (Map<String, Object>)jsonMap.get("listeners");
      if (map == null) {
        listeners = Collections.emptyMap();
      } else {
        listeners = new HashMap<>(map.size());
        for (Map.Entry<String, Object> entry : map.entrySet()) {
          listeners.put(entry.getKey(), new ListenerConfig((Map<String, Object>)entry.getValue()));
        }
      }
    }
    return listeners;
  }
}
