package org.apache.solr.handler.admin;

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


import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.PluginBag;
import org.apache.solr.core.SolrCore;
import org.apache.solr.v2api.Api;
import org.apache.solr.v2api.ApiBag;
import org.easymock.EasyMock;
import org.easymock.IAnswer;

import static org.easymock.EasyMock.getCurrentArguments;

public class TestCoreAdminApis extends SolrTestCaseJ4 {

  public void testCreate() throws Exception {
    Map<String, Object[]> calls = new HashMap<>();
    CoreContainer mockCC = getCoreContainerMock(calls, new HashMap<>());

    CoreAdminHandler  coreAdminHandler = new CoreAdminHandler(mockCC);
    ApiBag apiBag = new ApiBag();
    for (Api api : coreAdminHandler.getApis()) {
      apiBag.register(api, Collections.EMPTY_MAP);
    }
    TestCollectionAPIs.makeCall(apiBag, "/cores", SolrRequest.METHOD.POST,
        "{create:{name: hello, instanceDir : someDir, config : 'solrconfig.xml', schema: 'schema.xml'}}", mockCC);
    Object[] create = calls.get("create");
    assertEquals("hello" ,create[0]);
    HashMap expected = new HashMap();
    expected.put("schema", "schema.xml");
    expected.put("config", "solrconfig.xml");
    assertEquals(expected ,create[2]);

  }

  public static CoreContainer getCoreContainerMock(final Map<String, Object[]> in,Map<String,Object> out ) {
    CoreContainer mockCC = EasyMock.createMock(CoreContainer.class);
    EasyMock.reset(mockCC);
    mockCC.create(EasyMock.anyObject(String.class), EasyMock.anyObject(Path.class ) ,EasyMock.anyObject(Map.class));
    EasyMock.expectLastCall().andAnswer(new IAnswer<SolrCore>() {
      @Override
      public SolrCore answer() throws Throwable {
        in.put("create", getCurrentArguments());
        return null;
      }
    }).anyTimes();

    mockCC.getCoreRootDirectory();
    EasyMock.expectLastCall().andAnswer(new IAnswer<Path>() {
      @Override
      public Path answer() throws Throwable {
        return Paths.get("coreroot");
      }
    }).anyTimes();
    mockCC.getContainerProperties();
    EasyMock.expectLastCall().andAnswer(new IAnswer<Properties>() {
      @Override
      public Properties answer() throws Throwable {
        return new Properties();
      }
    }).anyTimes();

    mockCC.getRequestHandlers();
    EasyMock.expectLastCall().andAnswer(new IAnswer<PluginBag>() {
      @Override
      public PluginBag answer() throws Throwable {
        return (PluginBag)out.get("getRequestHandlers");
      }
    }).anyTimes();

    EasyMock.replay(mockCC);
    return mockCC;
  }


}
