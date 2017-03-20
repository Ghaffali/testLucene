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
package org.apache.solr.search;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.uninverting.UninvertingReader;

/**
 * A SolrInfoBean that provides introspection of the Solr FieldCache
 *
 */
public class SolrFieldCacheBean implements SolrInfoBean, SolrMetricProducer {

  private boolean disableEntryList = Boolean.getBoolean("disableSolrFieldCacheMBeanEntryList");

  private MetricsMap metricsMap;

  @Override
  public String getName() { return this.getClass().getName(); }
  @Override
  public String getDescription() {
    return "Provides introspection of the Solr FieldCache ";
  }
  @Override
  public Category getCategory() { return Category.CACHE; } 

  @Override
  public void initializeMetrics(SolrMetricManager manager, String registry, String scope) {
    metricsMap = detailed -> {
      Map<String, Object> map = new ConcurrentHashMap<>();
      if (detailed && !disableEntryList) {
        UninvertingReader.FieldCacheStats fieldCacheStats = UninvertingReader.getUninvertedStats();
        String[] entries = fieldCacheStats.info;
        map.put("entries_count", entries.length);
        map.put("total_size", fieldCacheStats.totalSize);
        for (int i = 0; i < entries.length; i++) {
          final String entry = entries[i];
          map.put("entry#" + i, entry);
        }
      } else {
        map.put("entries_count", UninvertingReader.getUninvertedStatsSize());
      }
      return map;
    };
    manager.register(registry, metricsMap, true, "fieldCache", Category.CACHE.toString(), scope);
  }
}
