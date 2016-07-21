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

package org.apache.solr.handler.admin;

import java.util.Collection;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.util.CommandOperation;

public interface ApiCommand<T> {
  String getName();

  SolrRequest.METHOD getHttpMethod();

  V2EndPoint getEndPoint();


 /* void command(SolrQueryRequest req, SolrQueryResponse rsp, CommandOperation c, T handler) throws Exception;

  void GET(SolrQueryRequest req, SolrQueryResponse rsp, T handler) throws Exception;
*/
  default Collection<String> getParamNames(CommandOperation op) {
    return BaseHandlerApiSupport.getParamNames(op, this);
  }


  default String getParamSubstitute(String name) {
    return name;
  }
}
