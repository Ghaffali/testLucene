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

package org.apache.solr.api;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.solr.common.util.ValidatingJsonMap;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.JsonSchemaValidator;

public abstract class Api implements SpecProvider {
  protected SpecProvider spec;
  protected volatile Map<String, JsonSchemaValidator> commandSchema;

  protected Api(SpecProvider spec) {
    this.spec = spec;
  }

  public Map<String, JsonSchemaValidator> getCommandSchema() {
    if (commandSchema == null) {
      synchronized (this) {
        if(commandSchema == null) {
          ValidatingJsonMap commands = getSpec().getMap("commands", null);
          commandSchema = commands != null ?
              ImmutableMap.copyOf(ApiBag.getParsedSchema(commands)) :
              ImmutableMap.of();
        }
      }
    }
    return commandSchema;
  }

  public abstract void call(SolrQueryRequest req , SolrQueryResponse rsp);

  @Override
  public ValidatingJsonMap getSpec() {
    return spec.getSpec();
  }

}
