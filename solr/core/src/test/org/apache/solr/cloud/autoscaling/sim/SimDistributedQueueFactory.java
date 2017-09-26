package org.apache.solr.cloud.autoscaling.sim;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import com.google.common.base.Preconditions;
import org.apache.solr.client.solrj.cloud.DistributedQueue;
import org.apache.solr.client.solrj.cloud.autoscaling.SolrCloudDataProvider;
import org.apache.solr.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SimDistributedQueueFactory implements SolrCloudDataProvider.DistributedQueueFactory {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  Map<String, SimDistributedQueue> queues = new ConcurrentHashMap<>();

  public SimDistributedQueueFactory() {
  }

  @Override
  public DistributedQueue makeQueue(String path) throws IOException {
    return queues.computeIfAbsent(path, p -> new SimDistributedQueue());
  }

  @Override
  public void removeQueue(String path) throws IOException {
    queues.remove(path);
  }

  public static class SimDistributedQueue implements DistributedQueue {
    private final Queue<Pair<String, byte[]>> queue = new ConcurrentLinkedQueue<>();
    private final ReentrantLock updateLock = new ReentrantLock();
    private final Condition changed = updateLock.newCondition();
    private int seq = 0;

    @Override
    public byte[] peek() throws Exception {
      Pair<String, byte[]> pair = queue.peek();
      return pair != null ? pair.second() : null;
    }

    @Override
    public byte[] peek(boolean block) throws Exception {
      return block ? peek(Long.MAX_VALUE) : peek();
    }

    @Override
    public byte[] peek(long wait) throws Exception {
      Pair<String, byte[]> pair = peekInternal(wait);
      return pair != null ? pair.second() : null;
    }

    private Pair<String, byte[]> peekInternal(long wait) throws Exception {
      Preconditions.checkArgument(wait > 0);
      long waitNanos = TimeUnit.MILLISECONDS.toNanos(wait);
      updateLock.lockInterruptibly();
      try {
        while (waitNanos > 0) {
          Pair<String, byte[]> pair = queue.peek();
          if (pair != null) {
            return pair;
          }
          waitNanos = changed.awaitNanos(waitNanos);
          if (waitNanos < 0) { // timed out
            return null;
          }
        }
      } finally {
        updateLock.unlock();
      }
      return null;
    }

    @Override
    public byte[] poll() throws Exception {
      updateLock.lockInterruptibly();
      try {
        Pair<String, byte[]>  pair = queue.poll();
        if (pair != null) {
          changed.signalAll();
          return pair.second();
        } else {
          return null;
        }
      } finally {
        updateLock.unlock();
      }
    }

    @Override
    public byte[] remove() throws Exception {
      updateLock.lockInterruptibly();
      try {
        byte[] res = queue.remove().second();
        changed.signalAll();
        return res;
      } finally {
        updateLock.unlock();
      }
    }

    @Override
    public byte[] take() throws Exception {
      updateLock.lockInterruptibly();
      try {
        while (true) {
          byte[] result = poll();
          if (result != null) {
            return result;
          }
          changed.await();
        }
      } finally {
        updateLock.unlock();
      }
    }

    @Override
    public void offer(byte[] data) throws Exception {
      updateLock.lockInterruptibly();
      try {
        queue.offer(new Pair(String.format("qn-%010d", seq), data));
        seq++;
        LOG.info("=== offer " + System.nanoTime());
        changed.signalAll();
      } finally {
        updateLock.unlock();
      }
    }

    @Override
    public Collection<Pair<String, byte[]>> peekElements(int max, long waitMillis, Predicate<String> acceptFilter) throws Exception {
      updateLock.lockInterruptibly();
      try {
        List<Pair<String, byte[]>> res = new LinkedList<>();
        final int maximum = max < 0 ? Integer.MAX_VALUE : max;
        final AtomicReference<Pair<String, byte[]>> pairRef = new AtomicReference<>();
        queue.forEach(pair -> {
          if (acceptFilter != null && !acceptFilter.test(pair.first())) {
            return;
          }
          if (res.size() < maximum) {
            pairRef.set(pair);
            res.add(pair);
          }
        });
        if (res.size() < maximum && waitMillis > 0) {
          long waitNanos = TimeUnit.MILLISECONDS.toNanos(waitMillis);
          waitNanos = changed.awaitNanos(waitNanos);
          if (waitNanos < 0) {
            return res;
          }
          AtomicBoolean seen = new AtomicBoolean(false);
          queue.forEach(pair -> {
            if (!seen.get()) {
              if (pairRef.get() == null) {
                seen.set(true);
              } else {
                if (pairRef.get().first().equals(pair.first())) {
                  seen.set(true);
                  return;
                }
              }
            }
            if (!seen.get()) {
              return;
            }
            if (!acceptFilter.test(pair.first())) {
              return;
            }
            if (res.size() < maximum) {
              res.add(pair);
              pairRef.set(pair);
            } else {
              return;
            }
          });
        }
        return res;
      } finally {
        updateLock.unlock();
      }
    }
  }
}
