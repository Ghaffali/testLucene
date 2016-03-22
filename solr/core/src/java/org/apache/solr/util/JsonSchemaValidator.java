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


import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class JsonSchemaValidator {
  private final Map jsonSchema;


  public JsonSchemaValidator(Map jsonSchema) {
    this.jsonSchema = jsonSchema;
    List<String> errs = new LinkedList<>();
    validateObjectDef(jsonSchema, errs);
    if(!errs.isEmpty()){
      throw new RuntimeException("Invalid schema. "+ StrUtils.join(errs,'|'));
    }
  }

  private  void validateObjectDef(Map jsonSchema, List<String> errs) {
    for (ObjectAttribute attr : ObjectAttribute.values()) {
      attr.validate(jsonSchema, errs);
    }
    jsonSchema.keySet().forEach(o -> {
      if (!knownAttributes.containsKey(o)) errs.add("Unknown key : " + o);
    });
    Map m = (Map) jsonSchema.get("properties");
    if(m != null){
      for (Object o : m.entrySet()) {
        Map.Entry e = (Map.Entry) o;
        if (e.getValue() instanceof Map) {
          Map od = (Map) e.getValue();
          validateObjectDef(od,errs);
        } else {
          errs.add("Invalid Object definition for field " +e.getKey());
        }
      }
    }

  }

  public List<String> validateJson(Map json) {
    return null;
  }

  enum ObjectAttribute {
    type(true, Type.STRING),
    properties(false, Type.OBJECT),
    additionalProperties(false, Type.BOOLEAN),
    required(false, Type.ARRAY),
    items(false,Type.OBJECT),
    __default(false,Type.UNKNOWN),
    description(false, Type.ARRAY),
    documentation(false, Type.STRING),
    oneOf(false, Type.ARRAY),
    id(false, Type.STRING),
    _ref(false, Type.STRING),
    _schema(false, Type.STRING);


    final String key;
    final boolean _required;
    final Type typ;

    public String getKey() {
      return key;
    }

    public void validate(Map attributeDefinition, List<String> errors) {
      Object val = attributeDefinition.get(key);
      if (val == null) {
        if (_required) errors.add("Missing required attribute '" + key+ "' in object "+ Utils.toJSONString(attributeDefinition) );
      } else {
        if (!typ.validate(val)) errors.add(key + " should be of type " + typ._name);
      }
    }

    ObjectAttribute(boolean required, Type type) {
      this.key = name().replaceAll("__","").replace('_', '$');
      this._required = required;
      this.typ = type;
    }
  }

  enum Type {
    STRING {
      @Override
      boolean validate(Object o) {
        return o instanceof String;
      }
    },
    ARRAY {
      @Override
      boolean validate(Object o) {
        return o instanceof List || o instanceof String;
      }
    },
    NUMBER {
      @Override
      boolean validate(Object o) {
        return o instanceof Number;
      }
    }, BOOLEAN {
      @Override
      boolean validate(Object o) {
        return o instanceof Boolean;
      }
    }, OBJECT {
      @Override
      boolean validate(Object o) {
        return o instanceof Map;
      }
    }, UNKNOWN {
      @Override
      boolean validate(Object o) {
        return true;
      }
    };
    final String _name;

    Type() {
      _name = this.name().toLowerCase(Locale.ROOT);
    }

    abstract boolean validate(Object o);

  }

  static final Map<String, ObjectAttribute> knownAttributes = unmodifiableMap(asList(ObjectAttribute.values()).stream().collect(toMap(ObjectAttribute::getKey, identity())));

}
