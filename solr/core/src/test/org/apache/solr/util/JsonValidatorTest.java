package org.apache.solr.util;

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

import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.api.ApiBag;
import org.apache.solr.common.util.Map2;

public class JsonValidatorTest extends SolrTestCaseJ4 {

  public void testSchema(){
    checkSchema("collections.commands");
    checkSchema("collections.collection.commands");
    checkSchema("collections.collection.shards.Commands");
    checkSchema("collections.collection.shards.shard.Commands");
    checkSchema("collections.collection.shards.shard.replica.Commands");
    checkSchema("cores.Commands");
    checkSchema("cores.core.Commands");
    checkSchema("cluster.security.BasicAuth.Commands");
    checkSchema("cluster.security.RuleBasedAuthorization");
    checkSchema("core.config.Commands");
    checkSchema("core.SchemaEdit");
  }

  private void checkSchema(String name) {
    Map2 spec = ApiBag.getSpec(name).getSpec();
    Map commands = (Map) spec.get("commands");
    for (Object o : commands.entrySet()) {
      Map.Entry cmd = (Map.Entry) o;
      try {
        new JsonSchemaValidator((Map) cmd.getValue());
      } catch (Exception e) {
        throw new RuntimeException("Error in command  "+ cmd.getKey() +" in schema "+name, e);
      }
    }
  }

}
