package org.apache.solr.cloud.autoscaling;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.cloud.DistributedQueue;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TriggerEventQueue extends DistributedQueue {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static class ReplayingEvent extends TriggerEventBase {

    public ReplayingEvent(AutoScaling.EventType eventType, String source, long eventNanoTime, Map<String, Object> properties) {
      super(eventType, source, eventNanoTime, properties);
      this.properties.put(REPLAYING, true);
    }
  }

  private final String triggerName;

  public TriggerEventQueue(SolrZkClient zookeeper, String triggerName, Overseer.Stats stats) {
    super(zookeeper, ZkStateReader.SOLR_AUTOSCALING_EVENTS_PATH + "/" + triggerName, stats);
    this.triggerName = triggerName;
  }

  public boolean offerEvent(AutoScaling.TriggerEvent event) {
    try {
      // yuck, serializing simple beans should be supported by Utils...
      Map<String, Object> map = new HashMap<>();
      map.put("eventType", event.getEventType().toString());
      map.put("source", event.getSource());
      map.put("eventNanoTime", event.getEventNanoTime());
      map.put("properties", event.getProperties());
      byte[] data = Utils.toJSON(map);
      offer(data);
      return true;
    } catch (KeeperException | InterruptedException e) {
      LOG.warn("Exception adding event " + event + " to queue " + triggerName, e);
      return false;
    }
  }

  public AutoScaling.TriggerEvent pollEvent() {
    byte[] data;
    try {
      while ((data = poll()) != null) {
        if (data.length == 0) {
          LOG.warn("ignoring empty data...");
          continue;
        }
        try {
          Map<String, Object> map = (Map<String, Object>) Utils.fromJSON(data);
          String source = (String)map.get("source");
          long eventNanoTime = ((Number)map.get("eventNanoTime")).longValue();
          AutoScaling.EventType eventType = AutoScaling.EventType.valueOf((String)map.get("eventType"));
          Map<String, Object> properties = (Map<String, Object>)map.get("properties");
          return new ReplayingEvent(eventType, source, eventNanoTime, properties);
        } catch (Exception e) {
          LOG.warn("Invalid event data, ignoring: " + new String(data));
          continue;
        }
      }
    } catch (KeeperException | InterruptedException e) {
      LOG.warn("Exception polling queue of trigger " + triggerName, e);
    }
    return null;
  }

}
