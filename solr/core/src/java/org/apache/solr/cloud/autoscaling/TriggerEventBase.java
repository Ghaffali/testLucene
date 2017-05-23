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
package org.apache.solr.cloud.autoscaling;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.util.IdUtils;

/**
 * Base class for event implementations.
 */
public abstract class TriggerEventBase implements AutoScaling.TriggerEvent {
  public static final String REPLAYING = "replaying";
  public static final String NODE_NAME = "nodeName";

  protected final String id;
  protected final String source;
  protected final long eventTime;
  protected final AutoScaling.EventType eventType;
  protected final Map<String, Object> properties = new HashMap<>();

  protected TriggerEventBase(AutoScaling.EventType eventType, String source, long eventTime,
                             Map<String, Object> properties) {
    this(IdUtils.timeRandomId(eventTime), eventType, source, eventTime, properties);
  }

  protected TriggerEventBase(String id, AutoScaling.EventType eventType, String source, long eventTime,
                             Map<String, Object> properties) {
    this.id = id;
    this.eventType = eventType;
    this.source = source;
    this.eventTime = eventTime;
    if (properties != null) {
      this.properties.putAll(properties);
    }
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getSource() {
    return source;
  }

  @Override
  public long getEventTime() {
    return eventTime;
  }

  @Override
  public Map<String, Object> getProperties() {
    return properties;
  }

  @Override
  public Object getProperty(String name) {
    return properties.get(name);
  }

  @Override
  public AutoScaling.EventType getEventType() {
    return eventType;
  }

  @Override
  public void setProperties(Map<String, Object> context) {
    this.properties.clear();
    if (context != null) {
      this.properties.putAll(context);
    }
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    ew.put("id", id);
    ew.put("source", source);
    ew.put("eventTime", eventTime);
    ew.put("eventType", eventType.toString());
    ew.put("properties", properties);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TriggerEventBase that = (TriggerEventBase) o;

    if (eventTime != that.eventTime) return false;
    if (!id.equals(that.id)) return false;
    if (!source.equals(that.source)) return false;
    if (eventType != that.eventType) return false;
    return properties.equals(that.properties);
  }

  @Override
  public int hashCode() {
    int result = id.hashCode();
    result = 31 * result + source.hashCode();
    result = 31 * result + (int) (eventTime ^ (eventTime >>> 32));
    result = 31 * result + eventType.hashCode();
    result = 31 * result + properties.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "{" +
        "id='" + id + '\'' +
        ", source='" + source + '\'' +
        ", eventTime=" + eventTime +
        ", properties=" + properties +
        '}';
  }
}
