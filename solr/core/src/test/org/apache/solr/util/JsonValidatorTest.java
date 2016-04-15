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

import java.util.List;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.api.ApiBag;
import org.apache.solr.common.util.Map2;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;

import static org.apache.solr.common.util.Map2.NOT_NULL;
import static org.apache.solr.common.util.Utils.toJSONString;

public class JsonValidatorTest extends SolrTestCaseJ4 {

  public void testSchema(){
    checkSchema("collections.commands");
    checkSchema("collections.collection.commands");
    checkSchema("collections.collection.shards.Commands");
    checkSchema("collections.collection.shards.shard.Commands");
    checkSchema("collections.collection.shards.shard.replica.Commands");
    checkSchema("cores.Commands");
    checkSchema("cores.core.Commands");
    checkSchema("node.Commands");
    checkSchema("cluster.security.BasicAuth.Commands");
    checkSchema("cluster.security.RuleBasedAuthorization");
    checkSchema("core.config.Commands");
    checkSchema("core.SchemaEdit");
  }



  public void testSchemaValidation() {
    Map2 spec = ApiBag.getSpec("collections.commands").getSpec();
    Map createSchema = spec.getMap("commands", NOT_NULL).getMap("create-alias", NOT_NULL);
    JsonSchemaValidator validator = new JsonSchemaValidator(createSchema);
    List<String> errs = validator.validateJson(Utils.fromJSONString("{name : x, collections: [ c1 , c2]}"));
    assertNull(toJSONString(errs), errs);
    errs = validator.validateJson(Utils.fromJSONString("{name : x, collections: c1 }"));
    assertNull(toJSONString(errs), errs);
    errs = validator.validateJson(Utils.fromJSONString("{name : x, x:y, collections: [ c1 , c2]}"));
    assertFalse(toJSONString(errs), errs.isEmpty());
    assertTrue(toJSONString(errs), errs.get(0).contains("Unknown"));
    errs = validator.validateJson(Utils.fromJSONString("{name : 123, collections: c1 }"));
    assertFalse(toJSONString(errs), errs.isEmpty());
    assertTrue(toJSONString(errs), errs.get(0).contains("Expected type"));
    errs = validator.validateJson(Utils.fromJSONString("{x:y, collections: [ c1 , c2]}"));
    assertEquals(toJSONString(errs),2, errs.size());
    assertTrue(toJSONString(errs), StrUtils.join(errs, '|').contains("Missing field"));
    assertTrue(toJSONString(errs), StrUtils.join(errs, '|').contains("Unknown"));
    errs = validator.validateJson(Utils.fromJSONString("{name : x, collections: [ 1 , 2]}"));
    assertFalse(toJSONString(errs), errs.isEmpty());
    assertTrue(toJSONString(errs), errs.get(0).contains("Expected elements of type"));
    Map schema = (Map) Utils.fromJSONString("{" +
        "  type:object," +
        "  properties: {" +
        "   age : {type: number}," +
        "   adult : {type: boolean}," +
        "   name: {type: string}}}");
    validator = new JsonSchemaValidator(schema);
    errs = validator.validateJson(Utils.fromJSONString("{name:x, age:21, adult:true}"));
    assertNull(errs);
    errs = validator.validateJson(Utils.fromJSONString("{name:x, age:'21', adult:'true'}"));
    assertNull(errs);

    errs = validator.validateJson(Utils.fromJSONString("{name:x, age:'x21', adult:'true'}"));
    assertEquals(1, errs.size());
    

  }

  private void checkSchema(String name) {
    Map2 spec = ApiBag.getSpec(name).getSpec();
    Map commands = (Map) spec.get("commands");
    for (Object o : commands.entrySet()) {
      Map.Entry cmd = (Map.Entry) o;
      try {
        JsonSchemaValidator validator = new JsonSchemaValidator((Map) cmd.getValue());
      } catch (Exception e) {
        throw new RuntimeException("Error in command  "+ cmd.getKey() +" in schema "+name, e);
      }
    }
  }

}
