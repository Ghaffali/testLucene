package org.apache.solr.v2api;

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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.Map2;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.PluginBag;
import org.apache.solr.core.SolrCore;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.servlet.HttpSolrCall;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.servlet.SolrRequestParsers;
import org.apache.solr.util.CommandOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.solr.common.params.CommonParams.JSON;
import static org.apache.solr.common.params.CommonParams.WT;
import static org.apache.solr.common.util.Map2.getDeepCopy;
import static org.apache.solr.common.util.StrUtils.formatString;
import static org.apache.solr.common.util.Map2.NOT_NULL;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.ADMIN;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.PROCESS;


public class V2HttpCall extends HttpSolrCall {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private V2Api api;
  private List<String> pieces;
  private String prefix;
  private String fullPath;
  HashMap<String, String> parts = new HashMap<>();
  static final Set<String> knownPrefixes = ImmutableSet.of("cluster", "node", "collections", "cores", "c");
  static final Set<String> commonPaths4ContainerLevelAndCoreLevel = ImmutableSet.of("collections", "cores", "c");

  public V2HttpCall(SolrDispatchFilter solrDispatchFilter, CoreContainer cc,
                    HttpServletRequest request, HttpServletResponse response, boolean retry) {
    super(solrDispatchFilter, cc, request, response, retry);
  }

  protected void init() throws Exception {
    fullPath = path = path.substring(3);//strip off '/v2'
    try {
      pieces = PathTrie.getParts(path);
      if (pieces.size() == 0) {
        prefix = "c";
        path = "/c";
      } else {
        prefix = pieces.get(0);
      }

      if (knownPrefixes.contains(prefix)) {
        api = getApiInfo(cores.getRequestHandlers(), path, req.getMethod(), cores, prefix, null, parts);
        if (api != null) {
          solrReq = SolrRequestParsers.DEFAULT.parse(null, path, req);
          solrReq.getContext().put(CoreContainer.class.getName(), cores);
          requestType = AuthorizationContext.RequestType.ADMIN;
          action = ADMIN;
          return;
        }
      }

      if ("c".equals(prefix) || "collections".equals(prefix)) {
        String collectionName = origCorename = corename = pieces.get(1);
        DocCollection collection = getDocCollection(collectionName);
        if (collection == null)
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "no such collection or alias");
        core = getCoreByCollection(collection.getName());
        if (core == null) {
          //this collection exists , but this node does not have a replica for that collection
          //todo find a better way to compute remote
          extractRemotePath(corename, origCorename, 0);
          return;
        }

      } else if ("cores".equals(prefix)) {
        origCorename = corename = pieces.get(1);
        core = cores.getCore(corename);
      }
      if (core == null)
        throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "no core retrieved for " + corename);


      path = path.substring(prefix.length() + pieces.get(1).length() + 2);
      api = getApiInfo(core.getRequestHandlers(), path, req.getMethod(), cores, prefix, fullPath, parts);
      MDCLoggingContext.setCore(core);
      parseRequest();

      if (usingAliases) {
        processAliases(aliases, collectionsList);
      }

      action = PROCESS;
      // we are done with a valid handler
    } catch (RuntimeException rte) {
      log.error("Error in init()", rte);
      throw rte;
    } finally {
      if (solrReq != null) solrReq.getContext().put(CommonParams.PATH, path);
    }
  }

  protected void parseRequest() throws Exception {
    config = core.getSolrConfig();
    // get or create/cache the parser for the core
    SolrRequestParsers parser = config.getRequestParsers();

    // With a valid handler and a valid core...

    if (solrReq == null) solrReq = parser.parse(core, path, req);
  }

  protected DocCollection getDocCollection(String collectionName) {
    if (!cores.isZooKeeperAware()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Solr not running in cloud mode ");
    }
    ZkStateReader zkStateReader = cores.getZkController().getZkStateReader();
    DocCollection collection = zkStateReader.getClusterState().getCollectionOrNull(collectionName);
    if (collection == null) {
      collectionName = corename = lookupAliases(collectionName);
      collection = zkStateReader.getClusterState().getCollectionOrNull(collectionName);
    }
    return collection;
  }

  public static V2Api getApiInfo(PluginBag<SolrRequestHandler> requestHandlers,
                                 String path, String method,
                                 CoreContainer cores, String prefix, String fullPath,
                                 Map<String, String> parts) {
    if (fullPath == null) fullPath = path;
    V2Api api;
    boolean containerHandlerLookup = cores.getRequestHandlers() == requestHandlers;
    api = requestHandlers.v2lookup(path, method, parts);
    if (api == null && path.endsWith(ApiBag.INTROSPECT)) {
      // the particular http method does not have any ,
      // just try if any other method has this path
      api = requestHandlers.v2lookup(path, null, parts);
    }

    if (api == null) {
      // this is to return the user with all the subpaths for  a given 4044 request
      // the request  begins with /collections , /cores or a /c and the current lookup is on container level handlers
      // So the subsequent per core lookup would find a path
      if (containerHandlerLookup && commonPaths4ContainerLevelAndCoreLevel.contains(prefix)) return null;


      Map<String, Set<String>> subpaths = new LinkedHashMap<>();

      getSubPaths(path, requestHandlers.getApiBag(), subpaths);
      if (!containerHandlerLookup) getSubPaths(fullPath, cores.getRequestHandlers().getApiBag(), subpaths);

      if (subpaths.isEmpty()) {
        throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "No valid handler for path :" + path);
      } else {
        return getSubPathImpl(subpaths, fullPath);
      }
    }
    if (api.getSpec() == ApiBag.INTROSPECT_SPEC)
      api = mergeIntrospect(requestHandlers, path, method, parts);
    return api;
  }

  private static void getSubPaths(String path, ApiBag bag, Map<String, Set<String>> pathsVsMethod) {
    for (SolrRequest.METHOD m : SolrRequest.METHOD.values()) {
      PathTrie<V2Api> registry = bag.getRegistry(m.toString());
      if (registry != null) {
        HashSet<String> subPaths = new HashSet<>();
        registry.lookup(path, new HashMap<>(), subPaths);
        for (String subPath : subPaths) {
          Set<String> supportedMethods = pathsVsMethod.get(subPath);
          if(supportedMethods == null) pathsVsMethod.put(subPath, supportedMethods = new HashSet<>());
          supportedMethods.add(m.toString());
        }
      }
    }
  }

  private static V2Api mergeIntrospect(PluginBag<SolrRequestHandler> requestHandlers,
                                       String path, String method, Map<String, String> parts) {
    V2Api api;
    final Map<String, V2Api> apis = new LinkedHashMap<>();
    for (String m : SolrRequest.SUPPORTED_METHODS) {
      api = requestHandlers.v2lookup(path, m, parts);
      if (api != null) apis.put(m, api);
    }
    api = new V2Api(ApiBag.INTROSPECT_SPEC) {
      @Override
      public void call(V2RequestContext ctx) {
        String method = ctx.getSolrRequest().getParams().get("method");
        Set<V2Api> added = new HashSet<>();
        for (Map.Entry<String, V2Api> e : apis.entrySet()) {
          if (method == null || e.getKey().equals(ctx.getHttpMethod())) {
            if (!added.contains(e.getValue())) {
              e.getValue().call(ctx);
              added.add(e.getValue());
            }
          }
        }
      }
    };
    return api;
  }

  private static V2Api getSubPathImpl(final Map<String, Set<String>> subpaths, String path) {
    return new V2Api(Map2.EMPTY) {
      @Override
      public void call(V2RequestContext ctx) {
        ctx.getResponse().add("msg", "Invalid path, try the following");
        LinkedHashMap<String, Set<String>> result = new LinkedHashMap<>(subpaths.size());
        for (Map.Entry<String, Set<String>> e : subpaths.entrySet()) {
          if (e.getKey().endsWith(ApiBag.INTROSPECT)) continue;
          result.put(path + e.getKey(), e.getValue());
        }
        ctx.getResponse().add("availableSubPaths", result);
      }
    };
  }

  @Override
  protected void handleAdmin(SolrQueryResponse solrResp) {
    api.call(getV2RequestCtx(solrResp));
  }

  @Override
  protected void execute(SolrQueryResponse rsp) {
    try {
      api.call(getV2RequestCtx(rsp));
    } catch (RuntimeException e) {
      //todo remove for debugging only
      log.error("error execute()", e);
      throw e;
    }
  }

  @Override
  protected QueryResponseWriter getResponseWriter() {
    String wt = solrReq.getParams().get(WT, JSON);
    if (core != null) return core.getResponseWriters().get(wt);
    return SolrCore.DEFAULT_RESPONSE_WRITERS.get(wt);
  }

  public V2RequestContext getV2RequestCtx(SolrQueryResponse solrResp) {

    return new V2RequestContext() {
      List<CommandOperation> parsedCommands;

      @Override
      public SolrQueryResponse getResponse() {
        return solrResp;
      }

      @Override
      public CoreContainer getCoreContainer() {
        return cores;
      }

      @Override
      public SolrQueryRequest getSolrRequest() {
        return solrReq;
      }

      @Override
      public String getPath() {
        return path;
      }

      @Override
      public String getHttpMethod() {
        return (String) solrReq.getContext().get("httpMethod");
      }

      @Override
      public Map<String, String> getPathValues() {
        return parts;
      }

      @Override
      public List<CommandOperation> getCommands(boolean validateInput) {
        if (parsedCommands == null) {
          Iterable<ContentStream> contentStreams = solrReq.getContentStreams();
          if (contentStreams == null) throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No content stream");
          for (ContentStream contentStream : contentStreams) {
            parsedCommands = getCommandOperations(new InputStreamReader((InputStream) contentStream, UTF_8),
                api.getSpec(), solrResp);

          }

        }
        return CommandOperation.clone(parsedCommands);

      }


    };
  }

  public static List<CommandOperation> getCommandOperations(Reader reader, Map2 spec, SolrQueryResponse rsp) {
    List<CommandOperation> parsedCommands = null;
    try {
      parsedCommands = CommandOperation.parse(reader);
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }

    Map2 cmds = spec.getMap("commands", NOT_NULL);
    List<CommandOperation> commandsCopy = CommandOperation.clone(parsedCommands);

    for (CommandOperation cmd : commandsCopy) {
      if (!cmds.containsKey(cmd.name)) {
        cmd.addError(formatString("Unknown operation ''{0}'' in path ''{1}''", cmd.name,
            spec.getMap("url", NOT_NULL).get("paths")));
      }
      //TODO validation

    }
    List<Map> errs = CommandOperation.captureErrors(commandsCopy);
    if (!errs.isEmpty()) {
      rsp.add(CommandOperation.ERR_MSGS, errs);
      SolrException exp = new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Errors in commands");
      rsp.setException(exp);
      throw exp;
    }
    return commandsCopy;
  }

}
