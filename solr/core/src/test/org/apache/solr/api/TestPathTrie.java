package org.apache.solr.api;

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


import java.util.HashMap;

import com.google.common.collect.ImmutableSet;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.util.PathTrie;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.apache.solr.api.ApiBag.HANDLER_NAME;

public class TestPathTrie extends SolrTestCaseJ4 {

  public void testPathTrie() {
    PathTrie<String> pathTrie = new PathTrie<>(ImmutableSet.of("_introspect"));
    pathTrie.insert("/", emptyMap(), "R");
    pathTrie.insert("/aa", emptyMap(), "d");
    pathTrie.insert("/aa/bb/{cc}/dd", emptyMap(), "a");
    pathTrie.insert("/$handlerName/{cc}/dd", singletonMap(HANDLER_NAME, "test"), "test");
    pathTrie.insert("/aa/bb/{cc}/{xx}", emptyMap(), "b");
    pathTrie.insert("/aa/bb", emptyMap(), "c");

    HashMap parts = new HashMap<>();
    assertEquals("R", pathTrie.lookup("/", parts, null));
    assertEquals("d", pathTrie.lookup("/aa", parts, null));
    assertEquals("a", pathTrie.lookup("/aa/bb/hello/dd", parts, null));
    assertEquals("test", pathTrie.lookup("/test/hello/dd", parts, null));
    assertEquals("hello", parts.get("cc"));
    assertEquals("b", pathTrie.lookup("/aa/bb/hello/world", parts, null));
    assertEquals("hello", parts.get("cc"));
    assertEquals("world", parts.get("xx"));


  }
}
