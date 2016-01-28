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


import java.util.Collection;
import java.util.Map;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.util.CommandOperation;
import org.apache.solr.v2api.V2RequestContext;

public interface V2Command<T> {
  String getName();

  SolrRequest.METHOD getHttpMethod();

  V2EndPoint getEndPoint();


  void command(V2RequestContext ctx, CommandOperation c, T handler) throws Exception;

  void GET(V2RequestContext ctx, T handler) throws Exception;

  Collection<String> getParamNames(CommandOperation op);

  String getParamSubstitute(String name);
}
