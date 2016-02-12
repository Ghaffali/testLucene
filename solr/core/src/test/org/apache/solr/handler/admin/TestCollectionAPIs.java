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


import java.io.StringReader;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.CommandOperation;
import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.api.V2HttpCall;
import org.apache.solr.api.V2RequestContext;

import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;

public class TestCollectionAPIs extends SolrTestCaseJ4 {

  public void testCreate() throws Exception{
    MockCollectionsHandler collectionsHandler = new MockCollectionsHandler();
    ApiBag apiBag = new ApiBag();
    Collection<Api> apis = collectionsHandler.getApis();
    for (Api api : apis) apiBag.register(api, Collections.EMPTY_MAP);
    //test a simple create collection call
    V2RequestContext ctx = makeCall(apiBag, "/collections", SolrRequest.METHOD.POST,
        "{create:{name:'newcoll', config:'schemaless', numShards:2, replicationFactor:2 }}", null);
    assertMapEqual((Map) Utils.fromJSONString("{name:newcoll, fromApi:'true', replicationFactor:'2', collection.configName:schemaless, numShards:'2', stateFormat:'2', operation:create}"),
        (ZkNodeProps) ctx.getSolrRequest().getContext().get(ZkNodeProps.class.getName()));

    //test a create collection with custom properties
    ctx = makeCall(apiBag, "/collections", SolrRequest.METHOD.POST,
        "{create:{name:'newcoll', config:'schemaless', numShards:2, replicationFactor:2, properties:{prop1:'prop1val', prop2: prop2val} }}", null);

    assertMapEqual(
        (Map) Utils.fromJSONString("{name:newcoll, fromApi:'true', replicationFactor:'2', collection.configName:schemaless, numShards:'2', stateFormat:'2', operation:create, property.prop1:prop1val, property.prop2:prop2val}"),
        (ZkNodeProps) ctx.getSolrRequest().getContext().get(ZkNodeProps.class.getName()));

  }

  public static V2RequestContext makeCall(final ApiBag apiBag, final String path, final SolrRequest.METHOD method,
                                    final String payload, final CoreContainer cc) throws Exception {
    final HashMap<String, String> parts = new HashMap<>();
    Api api = apiBag.lookup(path, method.toString(), parts);
    if (api == null) throw new RuntimeException("No handler at path :" + path);
    LocalSolrQueryRequest req = new LocalSolrQueryRequest(null, new MapSolrParams(new HashMap<>()));
    V2RequestContext ctx = getV2RequestContext(path, method, payload, cc, parts, api, req);
    api.call(ctx);
    return ctx;
  }

  public static V2RequestContext getV2RequestContext(final String path,
                                                     final SolrRequest.METHOD method,
                                                     final String payload,
                                                     final CoreContainer cc,
                                                     final HashMap<String, String> parts,
                                                     final Api api,
                                                     final LocalSolrQueryRequest req) {
    return new V2RequestContext() {
      SolrQueryResponse rsp = new SolrQueryResponse();

      @Override
      public SolrQueryResponse getResponse() {
        return rsp;
      }

      @Override
      public CoreContainer getCoreContainer() {
        return cc;
      }

      @Override
      public SolrQueryRequest getSolrRequest() {
        return req;
      }

      @Override
      public String getPath() {
        return path;
      }

      @Override
      public Map<String, String> getPathValues() {
        return parts;
      }

      @Override
      public List<CommandOperation> getCommands(boolean validateInput) {
        return V2HttpCall.getCommandOperations(new StringReader(payload), api.getSpec(), rsp);
      }

      @Override
      public String getHttpMethod() {
        return method.toString();
      }
    };
  }

  private void assertMapEqual(Map expected, ZkNodeProps actual) {
    assertEquals(expected.size(), actual.getProperties().size());
    for (Object o : expected.entrySet()) {
      Map.Entry e = (Map.Entry) o;
      assertEquals(e.getValue(), actual.get((String) e.getKey()));
    }
  }

  static class MockCollectionsHandler extends CollectionsHandler {
    LocalSolrQueryRequest req;

    MockCollectionsHandler() {

    }


    @Override
    protected void invokeAction(SolrQueryRequest req, SolrQueryResponse rsp, CollectionOperation operation) throws Exception {
      Map<String, Object> result = operation.call(req, rsp, this);
      if (result != null) {
        result.put(QUEUE_OPERATION, operation.action.toLower());
        req.getContext().put(ZkNodeProps.class.getName(),new ZkNodeProps(result) );
      }
    }
  }


}
