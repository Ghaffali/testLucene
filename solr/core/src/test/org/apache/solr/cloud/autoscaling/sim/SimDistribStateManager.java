package org.apache.solr.cloud.autoscaling.sim;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.jute.Record;
import org.apache.solr.client.solrj.cloud.autoscaling.DistribStateManager;
import org.apache.solr.cloud.ActionThrottle;
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
 *
 */
public class SimDistribStateManager implements DistribStateManager {

  private static final class Node {
    ReentrantLock dataLock = new ReentrantLock();
    private int version = 0;
    private int seq = 0;
    private final CreateMode mode;
    private final String path;
    private final String name;
    private final Node parent;
    private byte[] data = null;
    private Map<String, Node> children = new ConcurrentHashMap<>();
    List<Watcher> dataWatches = new ArrayList<>();
    List<Watcher> childrenWatches = new ArrayList<>();

    Node(Node parent, String name, String path, CreateMode mode) {
      this.parent = parent;
      this.name = name;
      this.path = path;
      this.mode = mode;
    }

    void setData(byte[] data, int version) throws IOException {
      dataLock.lock();
      try {
        if (version != -1 && version <= this.version) {
          throw new IOException("Version mismatch, existing=" + this.version + ", expected=" + version);
        }
        if (data != null) {
          this.data = Arrays.copyOf(data, data.length);
        } else {
          this.data = null;
        }
        if (version == -1) {
          this.version++;
        } else {
          this.version = version;
        }
        for (Watcher w : dataWatches) {
          w.process(new WatchedEvent(Watcher.Event.EventType.NodeDataChanged, Watcher.Event.KeeperState.SyncConnected, path));
        }
        dataWatches.clear();
      } finally {
        dataLock.unlock();
      }
    }

    VersionedData getData(Watcher w) {
      dataLock.lock();
      try {
        VersionedData res = new VersionedData(version, data);
        if (w != null && !dataWatches.contains(w)) {
          dataWatches.add(w);
        }
        return res;
      } finally {
        dataLock.unlock();
      }
    }

    void setChild(String name, Node child) {
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

    void removeChild(String name, int version) throws NoSuchElementException, IOException {
      Node n = children.get(name);
      if (n == null) {
        throw new NoSuchElementException(path + "/" + name);
      }
      if (version != -1 && version != n.version) {
        throw new IOException("Version mismatch, existing=" + this.version + ", expected=" + version);
      }
      for (Watcher w : childrenWatches) {
        w.process(new WatchedEvent(Watcher.Event.EventType.NodeChildrenChanged, Watcher.Event.KeeperState.SyncConnected, path));
      }
      for (Watcher w : n.dataWatches) {
        w.process(new WatchedEvent(Watcher.Event.EventType.NodeDeleted, Watcher.Event.KeeperState.SyncConnected, n.path));
      }
      // TODO: recurse and fire watches???
    }

  }

  public interface ActionError {
    boolean shouldFail(String path);
  }

  private final Node root = new Node(null, "", "/", CreateMode.PERSISTENT);

  private final ReentrantLock multiLock = new ReentrantLock();

  private final AtomicReference<ActionThrottle> throttleRef = new AtomicReference<>();
  private final AtomicReference<ActionError> errorRef = new AtomicReference<>();

  public SimDistribStateManager() {

  }

  public SimDistribStateManager(ActionThrottle actionThrottle, ActionError actionError) {
    this.throttleRef.set(actionThrottle);
    this.errorRef.set(actionError);
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
      String currentName = elements[1];
      currentPath.append('/');
      currentPath.append(currentName);
      n = parentNode.children.get(currentName);
      if (n == null) {
        if (create) {
          n = new Node(parentNode, currentName, currentPath.toString(), mode);
          parentNode.setChild(currentName, n);
        } else {
          break;
        }
      }
      parentNode = n;
    }
    return n;
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
  public void createData(String path, byte[] data, CreateMode mode) throws IOException {
    multiLock.lock();
    try {
      if (mode != CreateMode.PERSISTENT) {
        throw new UnsupportedOperationException("Mode " + mode + " not supported yet");
      }
      Node n = traverse(path, true, mode);
      n.setData(data, -1);
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
            s.setVersion(vd.version);
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
}
