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
package org.apache.solr.client.solrj.cloud.autoscaling;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Watcher;

/**
 * Represents a distributed state repository.
 */
public interface DistribStateManager extends Closeable {

  class VersionedData {
    public final int version;
    public final byte[] data;

    public VersionedData(int version, byte[] data) {
      this.version = version;
      this.data = data;
    }
  }

  default void close() throws IOException {

  }

  // state accessors

  boolean hasData(String path) throws IOException;

  List<String> listData(String path) throws NoSuchElementException, IOException;

  VersionedData getData(String path, Watcher watcher) throws NoSuchElementException, IOException;

  default VersionedData getData(String path) throws NoSuchElementException, IOException {
    return getData(path, null);
  }

  // state mutators

  void makePath(String path) throws IOException;

  /**
   * Create data (leaf) node at specified path.
   * @param path base path name of the node.
   * @param data data to be stored.
   * @param mode creation mode.
   * @return actual path of the node - in case of sequential nodes this will differ from the base path because
   * of the appended sequence number.
   * @throws IOException
   */
  String createData(String path, byte[] data, CreateMode mode) throws IOException;

  void removeData(String path, int version) throws NoSuchElementException, IOException;

  void setData(String path, byte[] data, int version) throws NoSuchElementException, IOException;

  List<OpResult> multi(final Iterable<Op> ops) throws IOException;

}
