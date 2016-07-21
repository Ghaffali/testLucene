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

import java.util.Arrays;
import java.util.List;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.CommandOperation;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.DELETE;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.GET;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.handler.admin.ConfigSetsHandler.ConfigSetOperation.CREATE_OP;
import static org.apache.solr.handler.admin.ConfigSetsHandler.ConfigSetOperation.DELETE_OP;
import static org.apache.solr.handler.admin.ConfigSetsHandler.ConfigSetOperation.LIST_OP;

public class ConfigSetsHandlerApi extends BaseHandlerApiSupport {

  private final ConfigSetsHandler configSetHandler;

  public ConfigSetsHandlerApi(ConfigSetsHandler configSetHandler) {
    this.configSetHandler = configSetHandler;
  }

  @Override
  protected void invokeCommand(SolrQueryRequest req, SolrQueryResponse rsp, ApiCommand command, CommandOperation c)
      throws Exception {
    ((Cmd) command).op.call(req, rsp, this.configSetHandler);

  }

  @Override
  protected void invokeUrl(ApiCommand command, SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    ((Cmd)command).op.call(req, rsp,configSetHandler);
  }

  @Override
  protected List<ApiCommand> getCommands() {
    return Arrays.asList(Cmd.values());
  }

  @Override
  protected List<V2EndPoint> getEndPoints() {
    return Arrays.asList(EndPoint.values());
  }

  enum Cmd implements ApiCommand<ConfigSetsHandler> {
    LIST(EndPoint.LIST_CONFIG, LIST_OP, GET),
    CREATE(EndPoint.CONFIG_COMMANDS, CREATE_OP, POST, "create"),
    DEL(EndPoint.CONFIG_DEL, DELETE_OP, DELETE)
    ;
    private final EndPoint endPoint;
    private final ConfigSetsHandler.ConfigSetOperation op;
    private final SolrRequest.METHOD method;
    private final String cmdName;

    Cmd(EndPoint endPoint, ConfigSetsHandler.ConfigSetOperation op, SolrRequest.METHOD method) {
      this(endPoint, op, method, null);
    }

    Cmd(EndPoint endPoint, ConfigSetsHandler.ConfigSetOperation op, SolrRequest.METHOD method, String cmdName) {
      this.cmdName = cmdName;
      this.endPoint = endPoint;
      this.op = op;
      this.method = method;
    }

    @Override
    public String getName() {
      return cmdName;
    }

    @Override
    public SolrRequest.METHOD getHttpMethod() {
      return method;
    }

    @Override
    public V2EndPoint getEndPoint() {
      return endPoint;
    }
  }
  enum EndPoint implements V2EndPoint {
    LIST_CONFIG("cluster.config"),
    CONFIG_COMMANDS("cluster.config.commands"),
    CONFIG_DEL("cluster.config.delete");

    public final String spec;

    EndPoint(String spec) {
      this.spec = spec;
    }

    @Override
    public String getSpecName() {
      return spec;
    }
  }
}
