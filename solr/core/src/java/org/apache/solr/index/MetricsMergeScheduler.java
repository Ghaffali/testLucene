package org.apache.solr.index;

import java.io.IOException;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.MergeTrigger;
import org.apache.solr.metrics.SolrMetricManager;

/**
 * Decorator for {@link MergeScheduler} that adds metrics for monitoring merge operations.
 */
public class MetricsMergeScheduler extends MergeScheduler {
  private final String registry;
  private final MergeScheduler in;

  private final Meter mergeSegmentFlush;
  private final Meter mergeFullFlush;
  private final Meter mergeExplicit;
  private final Meter mergeFinished;
  private final Meter mergeClosing;
  private final Counter mergeErrors;
  private final Timer mergeTimer;

  public MetricsMergeScheduler(String registry, MergeScheduler in) {
    this.registry = registry;
    this.in = in;
    mergeTimer = SolrMetricManager.timer(registry, "mergeTimer", "index");
    mergeSegmentFlush = SolrMetricManager.meter(registry, "mergeSegmentFlush", "index");
    mergeFullFlush = SolrMetricManager.meter(registry, "mergeFullFlush", "index");
    mergeExplicit = SolrMetricManager.meter(registry, "mergeExplicit", "index");
    // FINISHED is a confusing name... make it less confusing
    mergeFinished = SolrMetricManager.meter(registry, "mergeAfterMerge", "index");
    mergeClosing = SolrMetricManager.meter(registry, "mergeClosing", "index");
    mergeErrors = SolrMetricManager.counter(registry, "mergeErrors", "index");
  }

  @Override
  public void merge(IndexWriter writer, MergeTrigger trigger, boolean newMergesFound) throws IOException {
    Timer.Context context = null;
    if (newMergesFound) { // tick metrics only for real merges
      context = mergeTimer.time();
      switch (trigger) {
        case SEGMENT_FLUSH:
          mergeSegmentFlush.mark();
          break;
        case FULL_FLUSH:
          mergeFullFlush.mark();
          break;
        case EXPLICIT:
          mergeExplicit.mark();
          break;
        case MERGE_FINISHED:
          mergeFinished.mark();
          break;
        case CLOSING:
          mergeClosing.mark();
      }
    }
    try {
      in.merge(writer, trigger, newMergesFound);
    } catch (IOException e) {
      mergeErrors.inc();
      throw e;
    } finally {
      if (context != null) {
        context.close();
      }
    }
  }

  @Override
  public void close() throws IOException {
    in.close();
  }
}
