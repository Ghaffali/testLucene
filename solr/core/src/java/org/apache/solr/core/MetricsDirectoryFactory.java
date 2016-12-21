package org.apache.solr.core;

import java.io.IOException;
import java.util.Collection;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.metrics.SolrMetricManager;

/**
 * An implementation of {@link DirectoryFactory} that decorates provided factory by
 * adding metrics for directory IO operations.
 */
public class MetricsDirectoryFactory extends DirectoryFactory {
  private final SolrMetricManager metricManager;
  private final String registry;
  private final DirectoryFactory in;

  public MetricsDirectoryFactory(SolrMetricManager metricManager, String registry, DirectoryFactory in) {
    this.metricManager = metricManager;
    this.registry = registry;
    this.in = in;
  }

  public DirectoryFactory getDelegate() {
    return in;
  }

  @Override
  public void init(NamedList args) {
    in.init(args);
  }

  @Override
  public void doneWithDirectory(Directory dir) throws IOException {
    // unwrap
    if (dir instanceof MetricsDirectory) {
      dir = ((MetricsDirectory)dir).getDelegate();
    }
    in.doneWithDirectory(dir);
  }

  @Override
  public void addCloseListener(Directory dir, CachingDirectoryFactory.CloseListener closeListener) {
    // unwrap
    if (dir instanceof MetricsDirectory) {
      dir = ((MetricsDirectory)dir).getDelegate();
    }
    in.addCloseListener(dir, closeListener);
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  @Override
  protected Directory create(String path, LockFactory lockFactory, DirContext dirContext) throws IOException {
    Directory dir = in.create(path, lockFactory, dirContext);
    return new MetricsDirectory(metricManager, registry, dir);
  }

  @Override
  protected LockFactory createLockFactory(String rawLockType) throws IOException {
    return in.createLockFactory(rawLockType);
  }

  @Override
  public boolean exists(String path) throws IOException {
    return in.exists(path);
  }

  @Override
  public void remove(Directory dir) throws IOException {
    // unwrap
    if (dir instanceof MetricsDirectory) {
      dir = ((MetricsDirectory)dir).getDelegate();
    }
    in.remove(dir);
  }

  @Override
  public void remove(Directory dir, boolean afterCoreClose) throws IOException {
    // unwrap
    if (dir instanceof MetricsDirectory) {
      dir = ((MetricsDirectory)dir).getDelegate();
    }
    in.remove(dir, afterCoreClose);
  }

  @Override
  public void remove(String path, boolean afterCoreClose) throws IOException {
    in.remove(path, afterCoreClose);
  }

  @Override
  public void remove(String path) throws IOException {
    in.remove(path);
  }

  @Override
  public Directory get(String path, DirContext dirContext, String rawLockType) throws IOException {
    Directory dir = in.get(path, dirContext, rawLockType);
    if (dir instanceof MetricsDirectory) {
      return dir;
    } else {
      return new MetricsDirectory(metricManager, registry, dir);
    }
  }

  @Override
  public void incRef(Directory dir) {
    // unwrap
    if (dir instanceof MetricsDirectory) {
      dir = ((MetricsDirectory)dir).getDelegate();
    }
    in.incRef(dir);
  }

  @Override
  public boolean isPersistent() {
    return in.isPersistent();
  }

  @Override
  public void release(Directory dir) throws IOException {
    // unwrap
    if (dir instanceof MetricsDirectory) {
      dir = ((MetricsDirectory)dir).getDelegate();
    }
    in.release(dir);
  }

  private static final String SEGMENTS = "segments_";
  private static final String TEMP = "temp";
  private static final String OTHER = "other";

  public static class MetricsDirectory extends Directory {
    private final String PREFIX = "index.directory.";

    private final Directory in;
    private final String registry;
    private final SolrMetricManager metricManager;
    private final Meter totalReads;
    private final Meter totalWrites;

    public MetricsDirectory(SolrMetricManager metricManager, String registry, Directory in) throws IOException {
      this.metricManager = metricManager;
      this.registry = registry;
      this.in = in;
      this.totalReads = metricManager.meter(registry, "reads", SolrInfoMBean.Category.DIRECTORY.toString(), "total");
      this.totalWrites = metricManager.meter(registry, "writes", SolrInfoMBean.Category.DIRECTORY.toString(), "total");
    }

    public Directory getDelegate() {
      return in;
    }

    @Override
    public String[] listAll() throws IOException {
      return in.listAll();
    }

    @Override
    public void deleteFile(String name) throws IOException {
      in.deleteFile(name);
    }

    @Override
    public long fileLength(String name) throws IOException {
      return in.fileLength(name);
    }

    private String getMetricName(String name, boolean output) {
      String lastName;
      if (name.startsWith(SEGMENTS)) {
        lastName = SEGMENTS;
      } else {
        int pos = name.lastIndexOf('.');
        if (pos != -1 && name.length() > pos + 1) {
          lastName = name.substring(pos + 1);
        } else {
          lastName = OTHER;
        }
      }
      StringBuilder sb = new StringBuilder(PREFIX);
      sb.append(lastName);
      sb.append('.');
      if (output) {
        sb.append("write");
      } else {
        sb.append("read");
      }
      return sb.toString();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
      IndexOutput output = in.createOutput(name, context);
      if (output != null) {
        return new MetricsOutput(totalWrites, metricManager, registry, getMetricName(name, true), output);
      } else {
        return null;
      }
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
      IndexOutput output = in.createTempOutput(prefix, suffix, context);
      if (output != null) {
        return new MetricsOutput(totalWrites, metricManager, registry, getMetricName(TEMP, true), output);
      } else {
        return null;
      }
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
      in.sync(names);
    }

    @Override
    public void rename(String source, String dest) throws IOException {
      in.rename(source, dest);
    }

    @Override
    public void syncMetaData() throws IOException {
      in.syncMetaData();
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
      IndexInput input = in.openInput(name, context);
      if (input != null) {
        return new MetricsInput(totalReads, metricManager, registry, getMetricName(name, false), input);
      } else {
        return null;
      }
    }

    @Override
    public Lock obtainLock(String name) throws IOException {
      return in.obtainLock(name);
    }

    @Override
    public void close() throws IOException {
      in.close();
    }
  }

  public static class MetricsOutput extends IndexOutput {
    private final IndexOutput in;
    private final Histogram histogram;
    private final Meter meter;
    private final Meter totalMeter;

    public MetricsOutput(Meter totalMeter, SolrMetricManager metricManager,
                         String registry, String metricName, IndexOutput in) {
      super(in.toString(), in.getName());
      this.in = in;
      this.totalMeter = totalMeter;
      String histName = metricName + "Sizes";
      String meterName = metricName + "s";
      this.histogram = metricManager.histogram(registry, histName);
      this.meter = metricManager.meter(registry, meterName);
    }

    @Override
    public void writeByte(byte b) throws IOException {
      in.writeByte(b);
      totalMeter.mark();
      meter.mark();
      histogram.update(1);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
      in.writeBytes(b, offset, length);
      totalMeter.mark(length);
      meter.mark(length);
      histogram.update(length);
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public long getFilePointer() {
      return in.getFilePointer();
    }

    @Override
    public long getChecksum() throws IOException {
      return in.getChecksum();
    }
  }

  public static class MetricsInput extends IndexInput {
    private final IndexInput in;
    private final Histogram histogram;
    private final Meter meter;
    private final Meter totalMeter;

    public MetricsInput(Meter totalMeter, SolrMetricManager metricManager, String registry, String metricName, IndexInput in) {
      super(in.toString());
      this.in = in;
      this.totalMeter = totalMeter;
      String histName = metricName + "Sizes";
      String meterName = metricName + "s";
      this.histogram = metricManager.histogram(registry, histName);
      this.meter = metricManager.meter(registry, meterName);
    }

    public MetricsInput(Meter totalMeter, Histogram histogram, Meter meter, IndexInput in) {
      super(in.toString());
      this.in = in;
      this.totalMeter = totalMeter;
      this.histogram = histogram;
      this.meter = meter;
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public long getFilePointer() {
      return in.getFilePointer();
    }

    @Override
    public void seek(long pos) throws IOException {
      in.seek(pos);
    }

    @Override
    public long length() {
      return in.length();
    }

    @Override
    public IndexInput clone() {
      return new MetricsInput(totalMeter, histogram, meter, in.clone());
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      IndexInput slice = in.slice(sliceDescription, offset, length);
      if (slice != null) {
        return new MetricsInput(totalMeter, histogram, meter, slice);
      } else {
        return null;
      }
    }

    @Override
    public byte readByte() throws IOException {
      totalMeter.mark();
      meter.mark();
      histogram.update(1);
      return in.readByte();
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
      totalMeter.mark(len);
      meter.mark(len);
      histogram.update(len);
      in.readBytes(b, offset, len);
    }
  }
}
