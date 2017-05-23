package org.apache.solr.util;

import org.apache.lucene.util.StringHelper;

/**
 *
 */
public class IdUtils {

  /**
   * Generate a short random id (see {@link StringHelper#randomId()}).
   */
  public static final String randomId() {
    return StringHelper.idToString(StringHelper.randomId());
  }

  /**
   * Generate a random id with a timestamp, in the format:
   * <code>hex(timestamp) + 'T' + randomId</code>. This method
   * uses {@link System#currentTimeMillis()} as a timestamp.
   */
  public static final String timeRandomId() {
    return timeRandomId(System.currentTimeMillis());
  }

  /**
   * Generate a random id with a timestamp, in the format:
   * <code>hex(timestamp) + 'T' + randomId</code>.
   * @param time value representing timestamp
   */
  public static final String timeRandomId(long time) {
    StringBuilder sb = new StringBuilder(Long.toHexString(time));
    sb.append('T');
    sb.append(randomId());
    return sb.toString();
  }
}
