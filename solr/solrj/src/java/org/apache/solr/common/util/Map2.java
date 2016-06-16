package org.apache.solr.common.util;

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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableSet;

public class Map2<K, V> implements Map<K, V> {

  public static final Predicate<Object> NOT_NULL = o -> {
    if (o == null) return " Must not be NULL";
    return null;
  };
  public static final Predicate<Pair> NOT_NULL_OF_TYPE = pair -> {
    if (pair.first() == null) return " Must not be NULL";
    if (pair.second() instanceof Class) {
      return ((Class) pair.first()).isAssignableFrom(pair.first().getClass()) ?
          null :
          " Must be of type " + ((Class) pair.second()).getName();
    }
    return " Unknown Type";
  };
  public static final Predicate<Pair> ENUM_OF = pair -> {
    if (pair.second() instanceof Set) {
      Set set = (Set) pair.second();
      if (pair.first() instanceof Collection) {
        for (Object o : (Collection) pair.first()) {
          if (!set.contains(o)) {
            return " Must be one of " + pair.second();
          }
        }
      } else {
        if (!set.contains(pair.first())) return " Must be one of " + pair.second() + ", got " + pair.first();
      }
      return null;
    } else {
      return " Unknown type";
    }

  };
  private final Map<K, V> delegate;

  public Map2(Map<K, V> delegate) {
    this.delegate = delegate;
  }

  public Map2(int i) {
    delegate = new LinkedHashMap<>(i);
  }

  public Map2() {
    delegate = new LinkedHashMap<>();
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return delegate.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return delegate.containsValue(value);
  }

  @Override
  public V get(Object key) {
    return delegate.get(key);
  }

  @Override
  public V put(K key, V value) {
    return delegate.put(key, value);
  }

  @Override
  public V remove(Object key) {
    return delegate.remove(key);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    delegate.putAll(m);

  }

  @Override
  public void clear() {
    delegate.clear();

  }

  @Override
  public Set<K> keySet() {
    return delegate.keySet();
  }

  @Override
  public Collection<V> values() {
    return delegate.values();
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return delegate.entrySet();
  }

  public V get(K k, Predicate predicate) {
    V v = get(k);
    if (predicate != null) {
      String msg = predicate.test(v);
      if (msg != null) {
        throw new RuntimeException("" + k + msg);
      }
    }
    return v;
  }

  public Boolean getBool(K k, Boolean def) {
    V v = get(k);
    if (v == null) return def;
    if (v instanceof Boolean) return (Boolean) v;
    try {
      return Boolean.parseBoolean(v.toString());
    } catch (NumberFormatException e) {
      throw new RuntimeException("value of " + k + "must be an boolean");
    }
  }

  public Integer getInt(K k, Integer def) {
    V v = get(k);
    if (v == null) return def;
    if (v instanceof Integer) return (Integer) v;
    try {
      return Integer.parseInt(v.toString());
    } catch (NumberFormatException e) {
      throw new RuntimeException("value of " + k + "must be an integer");
    }
  }
  public Map2 getMap(String key, Predicate predicate) {
    return getMap(key, predicate, null);

  }

  public Map2 getMap(String key, Predicate predicate, String message) {
    V v = get(key);
    if (v != null && !(v instanceof Map)) {
      throw new RuntimeException("" + key + " should be of type map");
    }

    if (predicate != null) {
      String msg = predicate.test(v);
      if (msg != null) {
        msg = message != null ? message : key + msg;
        throw new RuntimeException(msg);
      }
    }
    return wrap((Map) v);
  }

  public List getList(String key, Predicate predicate) {
    return getList(key, predicate, null);
  }

  public List getList(String key, Predicate predicate, Object test) {
    V v = get(key);
    if (v != null && !(v instanceof List)) {
      throw new RuntimeException("" + key + " should be of type List");
    }

    if (predicate != null) {
      String msg = predicate.test(test == null ? v : new Pair(v, test));
      if (msg != null) {
        throw new RuntimeException("" + key + msg);
      }
    }

    return (List) v;
  }

  public V get(K k, Predicate<Pair> predicate, Object arg) {
    V v = get(k);
    String test = predicate.test(new Pair(v, arg));
    if (test != null) {
      throw new RuntimeException("" + k + test);
    }
    return v;
  }

  public V get(K k, V def) {
    V v = get(k);
    if (v == null) return def;
    return v;
  }

  static <K, V> Map2<K, V> wrap(Map<K, V> map) {
    if (map == null) return null;
    if (map instanceof Map2) {
      return (Map2) map;
    } else {
      return new Map2<>(map);
    }

  }

  public static Map2 fromJSON(InputStream is) {
    return fromJSON(new InputStreamReader(is, UTF_8));
  }

  public static Map2 fromJSON(Reader s) {
    try {
      return (Map2) (getObjectBuilder(new JSONParser(s)).getObject());
    } catch (IOException e) {
      throw new RuntimeException();
    }
  }

  public static Map2 getDeepCopy(Map map, int maxDepth, boolean mutable) {
    if (map == null) return null;
    if (maxDepth < 1) return Map2.wrap(map);
    Map2 copy = mutable ? new Map2(map.size()) : new Map2<>();
    for (Object o : map.entrySet()) {
      Map.Entry e = (Map.Entry) o;
      Object v = e.getValue();
      if (v instanceof Map) v = getDeepCopy((Map) v, maxDepth - 1, mutable);
      else if (v instanceof Collection) v = getDeepCopy((Collection) v, maxDepth - 1, mutable);
      copy.put(e.getKey(), v);
    }
    return mutable ? copy : new Map2<>(Collections.unmodifiableMap(copy));
  }

  public static Collection getDeepCopy(Collection c, int maxDepth, boolean mutable) {
    if (c == null || maxDepth < 1) return c;
    Collection result = c instanceof Set ? new HashSet() : new ArrayList();
    for (Object o : c) {
      if (o instanceof Map) {
        o = getDeepCopy((Map) o, maxDepth - 1, mutable);
      }
      result.add(o);
    }
    return mutable ? result : result instanceof Set ? unmodifiableSet((Set) result) : unmodifiableList((List) result);
  }

  private static ObjectBuilder getObjectBuilder(final JSONParser jp) throws IOException {
    return new ObjectBuilder(jp) {
      @Override
      public Object newObject() throws IOException {
        return new Map2();
      }
    };
  }


  @Override
  public boolean equals(Object that) {
    return that instanceof Map && this.delegate.equals(that);
  }

  public static final Map2 EMPTY = new Map2(Collections.EMPTY_MAP);
}
