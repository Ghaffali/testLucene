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

package org.apache.solr.cloud.autoscaling.sim;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.jute.Record;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.DistribStateManager;
import org.apache.solr.client.solrj.cloud.autoscaling.VersionedData;
import org.apache.solr.cloud.ActionThrottle;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.AutoScalingParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.IdUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.CheckVersionRequest;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.SetDataRequest;

/**
 * Simulated {@link DistribStateManager} that keeps all data locally in a static structure. Instances of this
 * class are identified by their id in order to simulate the deletion of ephemeral nodes when {@link #close()} is
 * invoked.
 */
public class SimDistribStateManager implements DistribStateManager {

  public static final class Node {
    ReentrantLock dataLock = new ReentrantLock();
    private int version = -1;
    private int seq = 0;
    private final CreateMode mode;
    private final String clientId;
    private final String path;
    private final String name;
    private final Node parent;
    private byte[] data = null;
    private Map<String, Node> children = new ConcurrentHashMap<>();
    List<Watcher> dataWatches = new ArrayList<>();
    List<Watcher> childrenWatches = new ArrayList<>();

    Node(Node parent, String name, String path, CreateMode mode, String clientId) {
      this.parent = parent;
      this.name = name;
      this.path = path;
      this.mode = mode;
      this.clientId = clientId;
    }

    public void clear() {
      dataLock.lock();
      try {
        children.clear();
        version = 0;
        seq = 0;
        dataWatches.clear();
        childrenWatches.clear();
        data = null;
      } finally {
        dataLock.unlock();
      }
    }

    public void setData(byte[] data, int version) throws IOException {
      dataLock.lock();
      try {
        if (version != -1 && version != this.version) {
          throw new IOException("Version mismatch, existing=" + this.version + ", expected=" + version);
        }
        if (data != null) {
          this.data = Arrays.copyOf(data, data.length);
        } else {
          this.data = null;
        }
        this.version++;
        for (Watcher w : dataWatches) {
          w.process(new WatchedEvent(Watcher.Event.EventType.NodeDataChanged, Watcher.Event.KeeperState.SyncConnected, path));
        }
        dataWatches.clear();
      } finally {
        dataLock.unlock();
      }
    }

    public VersionedData getData(Watcher w) {
      dataLock.lock();
      try {
        VersionedData res = new VersionedData(version, data, clientId);
        if (w != null && !dataWatches.contains(w)) {
          dataWatches.add(w);
        }
        return res;
      } finally {
        dataLock.unlock();
      }
    }

    public void setChild(String name, Node child) {
      assert child.name.equals(name);
      dataLock.lock();
      try {
        children.put(name, child);
        for (Watcher w : childrenWatches) {
          w.process(new WatchedEvent(Watcher.Event.EventType.NodeChildrenChanged, Watcher.Event.KeeperState.SyncConnected, path));
        }
        childrenWatches.clear();
      } finally {
        dataLock.unlock();
      }
    }

    public void removeChild(String name, int version) throws NoSuchElementException, IOException {
      Node n = children.get(name);
      if (n == null) {
        throw new NoSuchElementException(path + "/" + name);
      }
      if (version != -1 && version != n.version) {
        throw new IOException("Version mismatch, existing=" + this.version + ", expected=" + version);
      }
      children.remove(name);
      for (Watcher w : childrenWatches) {
        w.process(new WatchedEvent(Watcher.Event.EventType.NodeChildrenChanged, Watcher.Event.KeeperState.SyncConnected, path));
      }
      for (Watcher w : n.dataWatches) {
        w.process(new WatchedEvent(Watcher.Event.EventType.NodeDeleted, Watcher.Event.KeeperState.SyncConnected, n.path));
      }
      // TODO: not sure if it's correct to recurse and fire watches???
      Set<String> kids = new HashSet<>(n.children.keySet());
      for (String kid : kids) {
        n.removeChild(kid, -1);
      }
    }

    public void removeEphemeralChildren(String id) throws NoSuchElementException, IOException {
      Set<String> kids = new HashSet<>(children.keySet());
      for (String kid : kids) {
        Node n = children.get(kid);
        if (n == null) {
          continue;
        }
        if ((CreateMode.EPHEMERAL == n.mode || CreateMode.EPHEMERAL_SEQUENTIAL == n.mode) &&
            id.equals(n.clientId)) {
          removeChild(n.name, -1);
        } else {
          n.removeEphemeralChildren(id);
        }
      }
    }

  }

  public interface ActionError {
    boolean shouldFail(String path);
  }

  // shared state across all instances
  private static Node sharedRoot = createNewRootNode();
  private static final ReentrantLock multiLock = new ReentrantLock();

  public static Node createNewRootNode() {
    return new Node(null, "", "/", CreateMode.PERSISTENT, "__root__");
  }

  private final AtomicReference<ActionThrottle> throttleRef = new AtomicReference<>();
  private final AtomicReference<ActionError> errorRef = new AtomicReference<>();
  private final String id;
  private final Node root;

  public SimDistribStateManager() {
    this(null);
  }

  /**
   * Construct new state manager that uses provided root node for storing data.
   * @param root if null then a static shared root will be used.
   */
  public SimDistribStateManager(Node root) {
    this.id = IdUtils.timeRandomId();
    if (root != null) {
      this.root = root;
    } else {
      this.root = sharedRoot;
    }
  }

  public SimDistribStateManager(ActionThrottle actionThrottle, ActionError actionError) {
    this(null, actionThrottle, actionError);
  }

  public SimDistribStateManager(Node root, ActionThrottle actionThrottle, ActionError actionError) {
    this(root);
    this.throttleRef.set(actionThrottle);
    this.errorRef.set(actionError);
  }

  public void clear() {
    root.clear();
  }

  private void throttleOrError(String path) throws IOException {
    ActionError err = errorRef.get();
    if (err != null && err.shouldFail(path)) {
      throw new IOException("Simulated error, path=" + path);
    }
    ActionThrottle throttle = throttleRef.get();
    if (throttle != null) {
      throttle.minimumWaitBetweenActions();
      throttle.markAttemptingAction();
    }
  }

  private Node traverse(String path, boolean create, CreateMode mode) throws IOException {
    if (path == null || path.isEmpty()) {
      return null;
    }
    throttleOrError(path);
    if (path.charAt(0) == '/') {
      path = path.substring(1);
    }
    StringBuilder currentPath = new StringBuilder();
    String[] elements = path.split("/");
    Node parentNode = root;
    Node n = null;
    for (int i = 0; i < elements.length; i++) {
      String currentName = elements[i];
      currentPath.append('/');
      n = parentNode.children.get(currentName);
      if (n == null) {
        if (create) {
          if ((parentNode.mode == CreateMode.EPHEMERAL || parentNode.mode == CreateMode.EPHEMERAL_SEQUENTIAL) &&
              (mode == CreateMode.EPHEMERAL || mode == CreateMode.EPHEMERAL_SEQUENTIAL)) {
            throw new IOException("NoChildrenEphemerals for " + parentNode.path);
          }
          if (CreateMode.PERSISTENT_SEQUENTIAL == mode || CreateMode.EPHEMERAL_SEQUENTIAL == mode) {
            currentName = currentName + String.format("%010d", parentNode.seq);
            parentNode.seq++;
          }
          currentPath.append(currentName);
          n = new Node(parentNode, currentName, currentPath.toString(), mode, id);
          parentNode.setChild(currentName, n);
        } else {
          break;
        }
      } else {
        currentPath.append(currentName);
      }
      parentNode = n;
    }
    return n;
  }

  @Override
  public void close() throws IOException {
    multiLock.lock();
    try {
      // remove all my ephemeral nodes
      root.removeEphemeralChildren(id);
    } finally {
      multiLock.unlock();
    }

  }

  @Override
  public boolean hasData(String path) throws IOException {
    multiLock.lock();
    try {
      return traverse(path, false, CreateMode.PERSISTENT) != null;
    } finally {
      multiLock.unlock();
    }
  }

  @Override
  public List<String> listData(String path) throws NoSuchElementException, IOException {
    multiLock.lock();
    try {
      Node n = traverse(path, false, CreateMode.PERSISTENT);
      if (n == null) {
        throw new NoSuchElementException(path);
      }
      List<String> res = new ArrayList<>(n.children.keySet());
      Collections.sort(res);
      return res;
    } finally {
      multiLock.unlock();
    }
  }

  @Override
  public VersionedData getData(String path, Watcher watcher) throws NoSuchElementException, IOException {
    multiLock.lock();
    try {
      Node n = traverse(path, false, CreateMode.PERSISTENT);
      if (n == null) {
        throw new NoSuchElementException(path);
      }
      return n.getData(watcher);
    } finally {
      multiLock.unlock();
    }
  }

  @Override
  public void makePath(String path) throws IOException {
    multiLock.lock();
    try {
      traverse(path, true, CreateMode.PERSISTENT);
    } finally {
      multiLock.unlock();
    }
  }

  @Override
  public String createData(String path, byte[] data, CreateMode mode) throws NoSuchElementException, IOException {
    if ((CreateMode.EPHEMERAL == mode || CreateMode.PERSISTENT == mode) && hasData(path)) {
      throw new IOException("Path " + path + " already exists.");
    }
    // check if parent exists
    String relPath = path.charAt(0) == '/' ? path.substring(1) : path;
    if (relPath.length() > 0) { // non-root path - check if parent exists
      String[] elements = relPath.split("/");
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < elements.length - 1; i++) {
        sb.append('/');
        sb.append(elements[i]);
      }
      if (!hasData(sb.toString())) {
        throw new NoSuchElementException(sb.toString());
      }
    }
    multiLock.lock();
    try {
      Node n = traverse(path, true, mode);
      n.setData(data, -1);
      return n.path;
    } finally {
      multiLock.unlock();
    }
  }

  @Override
  public void removeData(String path, int version) throws NoSuchElementException, IOException {
    multiLock.lock();
    try {
      Node n = traverse(path, false, CreateMode.PERSISTENT);
      if (n == null) {
        throw new NoSuchElementException(path);
      }
      Node parent = n.parent;
      if (parent == null) {
        throw new IOException("Cannot remove root node");
      }
      parent.removeChild(n.name, version);
    } finally {
      multiLock.unlock();
    }

  }

  @Override
  public void setData(String path, byte[] data, int version) throws NoSuchElementException, IOException {
    multiLock.lock();
    try {
      Node n = traverse(path, false, CreateMode.PERSISTENT);
      if (n == null) {
        throw new NoSuchElementException(path);
      }
      n.setData(data, version);
    } finally {
      multiLock.unlock();
    }

  }

  @Override
  public List<OpResult> multi(Iterable<Op> ops) throws IOException {
    multiLock.lock();
    List<OpResult> res = new ArrayList<>();
    try {
      for (Op op : ops) {
        Record r = op.toRequestRecord();
        try {
          if (op instanceof Op.Check) {
            CheckVersionRequest rr = (CheckVersionRequest)r;
            Node n = traverse(rr.getPath(), false, CreateMode.PERSISTENT);
            if (n == null) {
              throw new NoSuchElementException(rr.getPath());
            }
            if (rr.getVersion() != -1 && n.version != rr.getVersion()) {
              throw new Exception("version mismatch");
            }
            // everything ok
            res.add(new OpResult.CheckResult());
          } else if (op instanceof Op.Create) {
            CreateRequest rr = (CreateRequest)r;
            createData(rr.getPath(), rr.getData(), CreateMode.fromFlag(rr.getFlags()));
            res.add(new OpResult.CreateResult(rr.getPath()));
          } else if (op instanceof Op.Delete) {
            DeleteRequest rr = (DeleteRequest)r;
            removeData(rr.getPath(), rr.getVersion());
            res.add(new OpResult.DeleteResult());
          } else if (op instanceof Op.SetData) {
            SetDataRequest rr = (SetDataRequest)r;
            setData(rr.getPath(), rr.getData(), rr.getVersion());
            VersionedData vd = getData(rr.getPath());
            Stat s = new Stat();
            s.setVersion(vd.getVersion());
            res.add(new OpResult.SetDataResult(s));
          } else {
            throw new Exception("Unknown Op: " + op);
          }
        } catch (Exception e) {
          res.add(new OpResult.ErrorResult(KeeperException.Code.APIERROR.intValue()));
        }
      }
    } finally {
      multiLock.unlock();
    }
    return res;
  }

  @Override
  public AutoScalingConfig getAutoScalingConfig(Watcher watcher) throws InterruptedException, IOException {
    Map<String, Object> map = new HashMap<>();
    int version = -1;
    try {
      VersionedData data = getData(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH, watcher);
      if (data != null && data.getData() != null && data.getData().length > 0) {
        map = (Map<String, Object>) Utils.fromJSON(data.getData());
      }
    } catch (NoSuchElementException e) {
      // ignore
    }
    map.put(AutoScalingParams.ZK_VERSION, version);
    return new AutoScalingConfig(map);
  }

  // ------------ simulator methods --------------

  public void simSetAutoScalingConfig(AutoScalingConfig cfg) throws Exception {
    try {
      makePath(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH);
    } catch (Exception e) {
      // ignore
    }
    setData(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH, Utils.toJSON(cfg), -1);
  }
}
