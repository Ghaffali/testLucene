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

package org.apache.solr.client.solrj.impl;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.solr.client.solrj.cloud.autoscaling.DistribStateManager;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

/**
 * Implementation of {@link DistribStateManager} that uses Zookeeper.
 */
public class ZkDistribStateManager implements DistribStateManager {

  private final SolrZkClient zkClient;

  public ZkDistribStateManager(SolrZkClient zkClient) {
    this.zkClient = zkClient;
  }

  @Override
  public boolean hasData(String path) throws IOException {
    try {
      return zkClient.exists(path, true);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public List<String> listData(String path) throws NoSuchElementException, IOException {
    try {
      return zkClient.getChildren(path, null, true);
    } catch (KeeperException.NoNodeException e) {
      throw new NoSuchElementException(path);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public DistribStateManager.VersionedData getData(String path, Watcher watcher) throws NoSuchElementException, IOException {
    Stat stat = new Stat();
    try {
      byte[] bytes = zkClient.getData(path, watcher, stat, true);
      return new DistribStateManager.VersionedData(stat.getVersion(), bytes);
    } catch (KeeperException.NoNodeException e) {
      throw new NoSuchElementException(path);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void makePath(String path) throws IOException {
    try {
      zkClient.makePath(path, true);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public String createData(String path, byte[] data, CreateMode mode) throws IOException {
    try {
      return zkClient.create(path, data, mode, true);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void removeData(String path, int version) throws NoSuchElementException, IOException {
    try {
      zkClient.delete(path, version, true);
    } catch (KeeperException.NoNodeException e) {
      throw new NoSuchElementException(path);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setData(String path, byte[] data, int version) throws NoSuchElementException, IOException {
    try {
      zkClient.setData(path, data, version, true);
    } catch (KeeperException.NoNodeException e) {
      throw new NoSuchElementException(path);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public List<OpResult> multi(Iterable<Op> ops) throws IOException {
    try {
      return zkClient.multi(ops, true);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

}
