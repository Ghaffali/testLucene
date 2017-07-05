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

import java.io.Closeable;

import org.apache.solr.core.CoreContainer;

/**
 * Implementations of this interface are notified of stages in event processing that they were
 * registered for. Note: instances may be closed and re-created on each auto-scaling config update.
 */
public interface TriggerListener extends Closeable {

  void init(CoreContainer coreContainer, AutoScalingConfig.TriggerListenerConfig config);

  AutoScalingConfig.TriggerListenerConfig getTriggerListenerConfig();

  /**
   * This method is called when either a particular <code>stage</code> or
   * <code>actionName</code> is reached during event processing.
   * @param stage {@link AutoScaling.EventProcessorStage} that this listener was registered for, or null
   * @param actionName {@link TriggerAction} name that this listener was registered for, or null
   * @param event current event being processed
   * @param message optional message
   */
  void onEvent(AutoScaling.EventProcessorStage stage, String actionName, TriggerEvent event, String message);
}
