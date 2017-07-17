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

package org.apache.solr.cloud.autoscaling;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;

/**
 * This action saves the computed action plan to the {@link CollectionAdminParams#SYSTEM_COLL} collection.
 */
public class LogPlanAction extends TriggerActionBase {
  public static final String SOURCE_FIELD = "source_s";
  public static final String SOURCE = LogPlanAction.class.getSimpleName();
  public static final String DOC_TYPE = "autoscaling_action";

  @Override
  public void process(TriggerEvent event, ActionContext actionContext) {
    CoreContainer container = actionContext.getCoreContainer();
    try (CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder()
        .withZkHost(container.getZkController().getZkServerAddress())
        .withHttpClient(container.getUpdateShardHandler().getHttpClient())
        .build()) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(CommonParams.TYPE, DOC_TYPE);
      doc.addField(SOURCE_FIELD, SOURCE);
      doc.addField("id", event.getId());
      doc.addField("event.type_s", event.getEventType().toString());
      doc.addField("event.source_s", event.getSource());
      doc.addField("event.time_l", event.getEventTime());
      doc.addField("timestamp", new Date());
      addMap("event.property.", doc, event.getProperties());
      addOperations(doc, (List<SolrRequest>)actionContext.getProperties().get("operations"));
      // add JSON versions of event and context
      String eventJson = Utils.toJSONString(event);
      String contextJson = Utils.toJSONString(actionContext);
      doc.addField("event_str", eventJson);
      doc.addField("context_str", contextJson);
      UpdateRequest req = new UpdateRequest();
      req.add(doc);
      cloudSolrClient.request(req, CollectionAdminParams.SYSTEM_COLL);
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Unexpected exception while processing event: " + event, e);
    }
  }

  private void addMap(String prefix, SolrInputDocument doc, Map<String, Object> map) {
    map.forEach((k, v) -> {
      if (v instanceof Collection) {
        for (Object o : (Collection)v) {
          doc.addField(prefix + k + "_ss", String.valueOf(o));
        }
      } else {
        doc.addField(prefix + k + "_s", String.valueOf(v));
      }
    });
  }

  private void addOperations(SolrInputDocument doc, List<SolrRequest> operations) {
    if (operations == null || operations.isEmpty()) {
      return;
    }
    for (SolrRequest req : operations) {
      SolrParams params = req.getParams();
      if (params == null) {
        continue;
      }
      // build a whitespace-separated param string
      StringJoiner paramJoiner = new StringJoiner(" ");
      paramJoiner.setEmptyValue("");
      for (Iterator<String> it = params.getParameterNamesIterator(); it.hasNext(); ) {
        final String name = it.next();
        final String [] values = params.getParams(name);
        for (String value : values) {
          paramJoiner.add(name + "=" + value);
        }
      }
      String paramString = paramJoiner.toString();
      if (!paramString.isEmpty()) {
        doc.addField("operations.params_ts", paramString);
      }
    }
  }
}
