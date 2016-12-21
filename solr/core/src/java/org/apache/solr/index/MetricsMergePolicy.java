package org.apache.solr.index;

import java.io.IOException;
import java.util.Map;

import com.codahale.metrics.Meter;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergePolicyWrapper;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.metrics.SolrMetricManager;

/**
 * A {@link MergePolicyWrapper} that wraps the provided {@link MergePolicy} and
 * adds metrics for reporting key operations using {@link org.apache.solr.metrics.SolrMetricManager}.
 */
public class MetricsMergePolicy extends MergePolicyWrapper {

  private final long sizeThresholdBytes;

  private final Meter minorMerges;
  private final Meter majorMerges;
  private final Meter majorDeletedDocs;
  private final Meter majorMergedDocs;
  private final Meter forcedMerges;
  private final Meter forcedDeletesMerges;

  /**
   * Creates a new merge policy instance with metrics.
   *
   * @param registry registry name where metrics are registered.
   * @param in the wrapped {@link MergePolicy}
   */
  public MetricsMergePolicy(SolrMetricManager metricManager, String registry, MergePolicy in) {
    super(in);
    // XXX make this configurable
    sizeThresholdBytes = 1024 * 1024;
    minorMerges = metricManager.meter(registry, "minorMerges", SolrInfoMBean.Category.INDEX.toString());
    majorMerges = metricManager.meter(registry, "majorMerges", SolrInfoMBean.Category.INDEX.toString());
    majorMergedDocs = metricManager.meter(registry, "majorMergedDocs", SolrInfoMBean.Category.INDEX.toString());
    majorDeletedDocs = metricManager.meter(registry, "majorDeletedDocs", SolrInfoMBean.Category.INDEX.toString());
    forcedMerges = metricManager.meter(registry, "forcedMerges", SolrInfoMBean.Category.INDEX.toString());
    forcedDeletesMerges = metricManager.meter(registry, "forcedDeletesMerges", SolrInfoMBean.Category.INDEX.toString());
  }

  @Override
  public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, IndexWriter writer) throws IOException {
    MergeSpecification merge = super.findMerges(mergeTrigger, segmentInfos, writer);
    tickMetrics(merge);
    return merge;
  }

  @Override
  public MergeSpecification findForcedMerges(SegmentInfos segmentInfos, int maxSegmentCount, Map<SegmentCommitInfo, Boolean> segmentsToMerge, IndexWriter writer) throws IOException {
    MergeSpecification merge = super.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, writer);
    if (merge != null) {
      forcedMerges.mark();
    }
    tickMetrics(merge);
    return merge;
  }

  @Override
  public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos, IndexWriter writer) throws IOException {
    MergeSpecification merge = super.findForcedDeletesMerges(segmentInfos, writer);
    if (merge != null) {
      forcedDeletesMerges.mark();
    }
    tickMetrics(merge);
    return merge;
  }

  private void tickMetrics(MergeSpecification merge) {
    if (merge == null || merge.merges.isEmpty()) {
      return;
    }
    long deletedDocs = 0;
    long totalNumDocs = 0;
    long totalBytesSize = 0;
    for (OneMerge one : merge.merges) {
      for (SegmentCommitInfo info : one.segments) {
        deletedDocs += info.getDelCount();
        try {
          totalBytesSize += info.sizeInBytes();
        } catch (IOException e) {
          // ignore - this should not cause fatal errors in merging!
        }
      }
      totalNumDocs += one.totalMaxDoc;
    }
    boolean major = totalBytesSize > sizeThresholdBytes;
    if (major) {
      majorMerges.mark();
      majorMergedDocs.mark(totalNumDocs);
      majorDeletedDocs.mark(deletedDocs);
    } else {
      minorMerges.mark();
    }
  }
}
