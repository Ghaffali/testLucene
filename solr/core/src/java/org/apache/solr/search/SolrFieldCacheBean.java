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

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.uninverting.UninvertingReader;

/**
 * A SolrInfoBean that provides introspection of the Solr FieldCache
 *
 */
public class SolrFieldCacheBean implements SolrInfoBean {

  private boolean disableEntryList = Boolean.getBoolean("disableSolrFieldCacheMBeanEntryList");
  private boolean disableJmxEntryList = Boolean.getBoolean("disableSolrFieldCacheMBeanEntryListJmx");

  @Override
  public String getName() { return this.getClass().getName(); }
  @Override
  public String getDescription() {
    return "Provides introspection of the Solr FieldCache ";
  }
  @Override
  public Category getCategory() { return Category.CACHE; } 
  @Override
  public NamedList getStatistics() {
    return getStats(!disableEntryList);
  }

  //nocommit
  //@Override
  public NamedList getStatisticsForJmx() {
    return getStats(!disableEntryList && !disableJmxEntryList);
  }

  private NamedList getStats(boolean listEntries) {
    NamedList stats = new SimpleOrderedMap();
    if (listEntries) {
      UninvertingReader.FieldCacheStats fieldCacheStats = UninvertingReader.getUninvertedStats();
      String[] entries = fieldCacheStats.info;
      stats.add("entries_count", entries.length);
      stats.add("total_size", fieldCacheStats.totalSize);
      for (int i = 0; i < entries.length; i++) {
        stats.add("entry#" + i, entries[i]);
      }
    } else {
      stats.add("entries_count", UninvertingReader.getUninvertedStatsSize());
    }
    return stats;
  }

}
