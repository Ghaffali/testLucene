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
package org.apache.solr.metrics.reporters.solr;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;

/**
 *
 */
public class MetricsReportRequest extends SolrRequest<SimpleSolrResponse> {
  private SimpleSolrResponse response = new SimpleSolrResponse();
  private SolrParams params;
  private List<ContentStream> contentStreams = null;
  private NamedList content;

  public MetricsReportRequest(String path, SolrParams params, NamedList content) {
    super(METHOD.POST, path);
    this.content = content;
    this.params = params;
  }

  @Override
  public SolrParams getParams() {
    return params;
  }

  @Override
  public Collection<ContentStream> getContentStreams() throws IOException {
    if (contentStreams == null) {
      final BinaryRequestWriter.BAOS baos = new BinaryRequestWriter.BAOS();
      new JavaBinCodec().marshal(content, baos);
      ContentStream cs = new ContentStream() {
        @Override
        public String getName() {
          return null;
        }

        @Override
        public String getSourceInfo() {
          return "javabin";
        }

        @Override
        public String getContentType() {
          return "application/javabin";
        }

        @Override
        public Long getSize() {
          return new Long(baos.size());
        }

        @Override
        public InputStream getStream() {
          return new ByteArrayInputStream(baos.getbuf(), 0, baos.size());
        }

        @Override
        public Reader getReader() {
          throw new RuntimeException("No reader available . this is a binarystream");
        }
      };
      contentStreams = Collections.singletonList(cs);
    }
    return contentStreams;
  }

  @Override
  protected SimpleSolrResponse createResponse(SolrClient client) {
    return response;
  }

}
