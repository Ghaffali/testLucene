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


import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.CommandOperation;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.*;
import static org.apache.solr.handler.admin.CoreAdminOperation.*;
import static org.apache.solr.handler.admin.CoreAdminHandlerApi.EndPoint.CORES_COMMANDS;
import static org.apache.solr.handler.admin.CoreAdminHandlerApi.EndPoint.CORES_STATUS;
import static org.apache.solr.handler.admin.CoreAdminHandlerApi.EndPoint.PER_CORE_COMMANDS;

public class CoreAdminHandlerApi extends BaseHandlerApiSupport {
  private final CoreAdminHandler handler;

  public CoreAdminHandlerApi(CoreAdminHandler handler) {
    this.handler = handler;
  }

  enum Cmd implements ApiCommand<CoreAdminHandlerApi> {
    CREATE(CORES_COMMANDS, POST, CREATE_OP, null, ImmutableMap.of("config", "configSet")),
    UNLOAD(PER_CORE_COMMANDS, POST, UNLOAD_OP, null, null),
    RELOAD(PER_CORE_COMMANDS, POST, RELOAD_OP, null, null),
    STATUS(CORES_STATUS, GET, STATUS_OP),
    SWAP(PER_CORE_COMMANDS, POST, SWAP_OP, null, ImmutableMap.of("other", "with")),
    RENAME(PER_CORE_COMMANDS, POST, RENAME_OP, null, null),
    MERGEINDEXES(PER_CORE_COMMANDS, POST, MERGEINDEXES_OP, null, null),
    SPLIT(PER_CORE_COMMANDS, POST, SPLIT_OP, null, ImmutableMap.of("split.key", "splitKey")),
    PREPRECOVERY(PER_CORE_COMMANDS, POST, PREPRECOVERY_OP, null, null),
    REQUESTRECOVERY(PER_CORE_COMMANDS, POST, REQUESTRECOVERY_OP, null, null),
    REQUESTSYNCSHARD(PER_CORE_COMMANDS, POST, REQUESTRECOVERY_OP, null, null),
    REQUESTBUFFERUPDATES(PER_CORE_COMMANDS, POST, REQUESTBUFFERUPDATES_OP, null, null),
    REQUESTAPPLYUPDATES(PER_CORE_COMMANDS, POST, REQUESTAPPLYUPDATES_OP, null, null),
    REQUESTSTATUS(PER_CORE_COMMANDS, POST, REQUESTSTATUS_OP, null, null),
    OVERSEEROP(PER_CORE_COMMANDS, POST, OVERSEEROP_OP, null, null),
    REJOINLEADERELECTION(PER_CORE_COMMANDS, POST, REJOINLEADERELECTION_OP, null, null),
    INVOKE(PER_CORE_COMMANDS, POST, INVOKE_OP, null, null),
    FORCEPREPAREFORLEADERSHIP(PER_CORE_COMMANDS, POST, FORCEPREPAREFORLEADERSHIP_OP, null, null);

    public final String commandName;
    public final EndPoint endPoint;
    public final SolrRequest.METHOD method;
    public final Map<String, String> paramstoAttr;
    final CoreAdminOperation target;


    Cmd(EndPoint endPoint, SolrRequest.METHOD method, CoreAdminOperation target) {
      this.endPoint = endPoint;
      this.method = method;
      this.target = target;
      commandName = null;
      paramstoAttr = Collections.EMPTY_MAP;

    }


    Cmd(EndPoint endPoint, SolrRequest.METHOD method, CoreAdminOperation target, String commandName,
        Map<String, String> paramstoAttr) {
      this.commandName = commandName == null ? target.action.toString().toLowerCase(Locale.ROOT) : commandName;
      this.endPoint = endPoint;
      this.method = method;
      this.target = target;
      this.paramstoAttr = paramstoAttr == null ? Collections.EMPTY_MAP : paramstoAttr;
    }

    @Override
    public String getName() {
      return commandName;
    }

    @Override
    public SolrRequest.METHOD getHttpMethod() {
      return method;
    }

    @Override
    public V2EndPoint getEndPoint() {
      return endPoint;
    }

    @Override
    public void command(SolrQueryRequest req, SolrQueryResponse rsp, CommandOperation c, CoreAdminHandlerApi handler) throws Exception {
      target.call(new CoreAdminHandler.CallInfo(handler.handler,req,rsp,target ));

    }

    @Override
    public void GET(SolrQueryRequest req, SolrQueryResponse rsp, CoreAdminHandlerApi handler) throws Exception {
      target.call(new CoreAdminHandler.CallInfo(handler.handler,req,rsp,target ));

    }

    @Override
    public Collection<String> getParamNames(CommandOperation op) {
      return BaseHandlerApiSupport.getParamNames(op, this);
    }

    @Override
    public String getParamSubstitute(String param) {
      return paramstoAttr.containsKey(param) ? paramstoAttr.get(param) : param;
    }
  }



  enum EndPoint implements V2EndPoint {
    CORES_STATUS("cores.Status"),
    CORES_COMMANDS("cores.Commands"),
    PER_CORE_COMMANDS("cores.core.Commands");

    final String specName;

    EndPoint(String specName) {
      this.specName = specName;
    }

    @Override
    public String getSpecName() {
      return specName;
    }
  }


  @Override
  protected void invokeCommand(SolrQueryRequest req, SolrQueryResponse rsp, ApiCommand command,
                               CommandOperation c) throws Exception {
    ((Cmd) command).command(req,rsp, c, this);
  }

  @Override
  protected void invokeUrl(ApiCommand command, SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    command.GET(req,rsp, this);
  }

  @Override
  protected List<ApiCommand> getCommands() {
    return Arrays.asList(Cmd.values());
  }

  @Override
  protected List<V2EndPoint> getEndPoints() {
    return Arrays.asList(EndPoint.values());
  }


}
