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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.cloud.OverseerCollectionMessageHandler;
import org.apache.solr.handler.admin.CollectionsHandler.CollectionOperation;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.CommandOperation;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.DELETE;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.GET;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.handler.admin.CollectionsHandler.CollectionOperation.*;


public class CollectionHandlerApi extends BaseHandlerApiSupport {
  private final CollectionsHandler handler;

  public CollectionHandlerApi(CollectionsHandler handler) {
    this.handler = handler;
  }

  @Override
  protected List<ApiCommand> getCommands() {
    return Arrays.asList(Cmd.values());
  }

  @Override
  protected void invokeCommand(SolrQueryRequest req, SolrQueryResponse rsp, ApiCommand command, CommandOperation c) throws Exception {
    ((Cmd) command).command(req, rsp,c, this);
  }

  @Override
  protected void invokeUrl(ApiCommand command, SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    ((Cmd) command).GET(req,rsp, this);
  }

  @Override
  protected List<V2EndPoint> getEndPoints() {
    return Arrays.asList(EndPoint.values());
  }


  enum Cmd implements ApiCommand<CollectionHandlerApi> {
    GET_COLLECTIONS(EndPoint.COLLECTIONS, GET, LIST_OP),
    GET_A_COLLECTION(EndPoint.COLLECTION_STATE, GET, CLUSTERSTATUS_OP),
    CREATE_COLLECTION(EndPoint.COLLECTIONS_COMMANDS,
        POST,
        CREATE_OP,
        CREATE_OP.action.toLower(),
        ImmutableMap.of(
            OverseerCollectionMessageHandler.COLL_CONF, "config"),
        ImmutableMap.of("properties.", "property.")),

    DELETE_COLL(EndPoint.PER_COLLECTION_DELETE,
        DELETE,
        DELETE_OP,
        DELETE_OP.action.toLower(),
        ImmutableMap.of(NAME, "collection")),

    RELOAD_COLL(EndPoint.PER_COLLECTION,
        POST,
        RELOAD_OP,
        RELOAD_OP.action.toLower(),
        ImmutableMap.of(NAME, "collection")),

    MIGRATE_DOCS(EndPoint.PER_COLLECTION,
        POST,
        MIGRATE_OP,
        "migrate-docs",
        ImmutableMap.of("split.key", "splitKey",
            "target.collection", "target",
            "forward.timeout", "forwardTimeout"
        )),
    CREATE_ALIAS(EndPoint.COLLECTIONS_COMMANDS,
        POST,
        CREATEALIAS_OP,
        "create-alias",
        null),

    DELETE_ALIAS(EndPoint.COLLECTIONS_COMMANDS,
        POST,
        DELETEALIAS_OP,
        "delete-alias",
        ImmutableMap.of(NAME, "")),
    CREATE_SHARD(EndPoint.PER_COLLECTION_SHARDS_COMMANDS,
        POST,
        CREATESHARD_OP,
        "create",
        null),

    SPLIT_SHARD(EndPoint.PER_COLLECTION_SHARDS_COMMANDS,
        POST,
        SPLITSHARD_OP,
        "split",
        ImmutableMap.of(
            "split.key", "splitKey"),
        ImmutableMap.of("coreProperties.", "property.")),
    DELETE_SHARD(EndPoint.PER_COLLECTION_PER_SHARD_DELETE,
        DELETE,
        DELETESHARD_OP),

    CREATE_REPLICA(EndPoint.PER_COLLECTION_PER_SHARD_COMMANDS,
        POST,
        ADDREPLICA_OP,
        "create-replica",
        null),

    DELETE_REPLICA(EndPoint.PER_COLLECTION_PER_SHARD_PER_REPLICA_DELETE,
        DELETE,
        DELETEREPLICA_OP),

    SYNC_SHARD(EndPoint.PER_COLLECTION_PER_SHARD_COMMANDS,
        POST,
        SYNCSHARD_OP,
        "synch-shard",
        null),;
    public final String commandName;
    public final EndPoint endPoint;
    public final SolrRequest.METHOD method;
    public final CollectionOperation target;
    public final Map<String, String> paramstoAttr;
    public final Map<String, String> prefixSubStitutes;

    public SolrRequest.METHOD getMethod() {
      return method;
    }


    Cmd(EndPoint endPoint, SolrRequest.METHOD method, CollectionOperation target) {
      this(endPoint, method, target, null, null);
    }

    Cmd(EndPoint endPoint, SolrRequest.METHOD method, CollectionOperation target,
        String commandName, Map<String, String> paramstoAttr) {
      this(endPoint, method, target, commandName, paramstoAttr, Collections.EMPTY_MAP);

    }

    Cmd(EndPoint endPoint, SolrRequest.METHOD method, CollectionOperation target,
        String commandName, Map<String, String> paramstoAttr, Map<String, String> prefixSubStitutes) {
      this.commandName = commandName;
      this.endPoint = endPoint;
      this.method = method;
      this.target = target;
      this.paramstoAttr = paramstoAttr == null ? Collections.EMPTY_MAP : paramstoAttr;
      this.prefixSubStitutes = prefixSubStitutes;

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
    public void command(SolrQueryRequest req, SolrQueryResponse rsp, CommandOperation c, CollectionHandlerApi handler) throws Exception {
      handler.handler.invokeAction(req, rsp, handler.handler.coreContainer, target.action,target);
    }

    @Override
    public void GET(SolrQueryRequest req, SolrQueryResponse rsp, CollectionHandlerApi handler) throws Exception {
      handler.handler.invokeAction(req, rsp, handler.handler.coreContainer, target.action,target);
    }

    @Override
    public Collection<String> getParamNames(CommandOperation op) {
      Collection<String> paramNames = BaseHandlerApiSupport.getParamNames(op, this);
      if (!prefixSubStitutes.isEmpty()) {
        Collection<String> result = new ArrayList<>(paramNames.size());
        for (Map.Entry<String, String> e : prefixSubStitutes.entrySet()) {
          for (String paramName : paramNames) {
            if (paramName.startsWith(e.getKey())) {
              result.add(paramName.replace(e.getKey(), e.getValue()));
            } else {
              result.add(paramName);
            }
          }
          paramNames = result;
        }
      }

      return paramNames;
    }

    @Override
    public String getParamSubstitute(String param) {
      String s = paramstoAttr.containsKey(param) ? paramstoAttr.get(param) : param;
      if (prefixSubStitutes != null) {
        for (Map.Entry<String, String> e : prefixSubStitutes.entrySet()) {
          if (s.startsWith(e.getValue())) return s.replace(e.getValue(), e.getKey());
        }
      }
      return s;
    }

  }

  enum EndPoint implements V2EndPoint {
    CLUSTER("cluster"),
    COLLECTIONS_COMMANDS("collections.Commands"),
    COLLECTIONS("collections"),
    COLLECTION_STATE("collection"),
    PER_COLLECTION("collections.collection.Commands"),
    PER_COLLECTION_DELETE("collections.collection.delete"),
    PER_COLLECTION_SHARDS_COMMANDS("collections.collection.shards.Commands"),
    PER_COLLECTION_PER_SHARD_COMMANDS("collections.collection.shards.shard.Commands"),
    PER_COLLECTION_PER_SHARD_DELETE("collections.collection.shards.shard.delete"),
    PER_COLLECTION_PER_SHARD_PER_REPLICA_COMMANDS("collections.collection.shards.shard.replica.Commands"),
    PER_COLLECTION_PER_SHARD_PER_REPLICA_DELETE("collections.collection.shards.shard.replica.delete");
    final String specName;


    EndPoint(String specName) {
      this.specName = specName;
    }

    @Override
    public String getSpecName() {
      return specName;
    }
  }

}
