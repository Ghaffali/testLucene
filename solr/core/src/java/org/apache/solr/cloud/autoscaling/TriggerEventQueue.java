package org.apache.solr.cloud.autoscaling;

import java.lang.invoke.MethodHandles;
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

  public static class QueuedEvent extends TriggerEventBase {

    public QueuedEvent(String id, AutoScaling.EventType eventType, String source, long eventTime, Map<String, Object> properties) {
      super(id, eventType, source, eventTime, properties);
    }
  }

  private final String triggerName;

  public TriggerEventQueue(SolrZkClient zookeeper, String triggerName, Overseer.Stats stats) {
    super(zookeeper, ZkStateReader.SOLR_AUTOSCALING_EVENTS_PATH + "/" + triggerName, stats);
    this.triggerName = triggerName;
  }

  public boolean offerEvent(AutoScaling.TriggerEvent event) {
    try {
      byte[] data = Utils.toJSON(event);
      offer(data);
      return true;
    } catch (KeeperException | InterruptedException e) {
      LOG.warn("Exception adding event " + event + " to queue " + triggerName, e);
      return false;
    }
  }

  public AutoScaling.TriggerEvent peekEvent() {
    byte[] data;
    try {
      while ((data = peek()) != null) {
        if (data.length == 0) {
          LOG.warn("ignoring empty data...");
          continue;
        }
        try {
          Map<String, Object> map = (Map<String, Object>) Utils.fromJSON(data);
          return fromMap(map);
        } catch (Exception e) {
          LOG.warn("Invalid event data, ignoring: " + new String(data));
          continue;
        }
      }
    } catch (KeeperException | InterruptedException e) {
      LOG.warn("Exception peeking queue of trigger " + triggerName, e);
    }
    return null;
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
          return fromMap(map);
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

  private static QueuedEvent fromMap(Map<String, Object> map) {
    String id = (String)map.get("id");
    String source = (String)map.get("source");
    long eventTime = ((Number)map.get("eventTime")).longValue();
    AutoScaling.EventType eventType = AutoScaling.EventType.valueOf((String)map.get("eventType"));
    Map<String, Object> properties = (Map<String, Object>)map.get("properties");
    return new QueuedEvent(id, eventType, source, eventTime, properties);
  }
}
