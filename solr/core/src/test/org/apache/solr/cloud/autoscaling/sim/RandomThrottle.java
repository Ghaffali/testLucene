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
package org.apache.solr.cloud.autoscaling.sim;

import java.util.concurrent.ExecutorService;

import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;

/**
 *
 */
public class RandomThrottle {

  private final int minMs;
  private final RealDistribution distribution;

  public RandomThrottle(int minMs, int maxMs) {
    this.minMs = minMs;
    this.distribution = new UniformRealDistribution(0, maxMs - minMs);
  }

  public RandomThrottle(int minMs, RealDistribution distribution) {
    this.minMs = minMs;
    this.distribution = distribution;
  }

  public void throttle() {
    double random = distribution.sample();
    if (random < 0) {
      random = 0;
    }
    try {
      Thread.sleep(minMs + Math.round(random));
    } catch (InterruptedException e) {
      // do nothing
    }
  }

  public void throttle(ExecutorService executor, Runnable runnable) {
    executor.submit(() -> {
      throttle();
      runnable.run();
    });
  }
}
