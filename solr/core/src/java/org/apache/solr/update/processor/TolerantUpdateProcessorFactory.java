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
package org.apache.solr.update.processor;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.DistributedUpdateProcessor.DistribPhase;

import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

/**
 * <p> 
 * Suppresses errors for individual add/delete commands within a batch.
 * Instead, all errors are logged and the batch continues. The client
 * will receive a 200 response, but gets a list of errors (keyed by
 * unique key) unless <code>maxErrors</code> is reached. 
 * If <code>maxErrors</code> occur, the last exception caught will be re-thrown, 
 * Solr will respond with 5XX or 4XX (depending on the exception) and
 * it won't finish processing the batch. This means that the last docs
 * in the batch may not be added in this case even if they are valid. 
 * Note that this UpdateRequestProcessor will only catch exceptions that occur 
 * on later elements in the chain.  
 * 
 * </p>
 * 
 * <p>
 * <code>maxErrors</code> is an int value that can be specified in the 
 * configuration and can also be overridden per request. If unset, it will 
 * default to <code>Integer.MAX_VALUE</code>
 * </p>
 * 
 * An example configuration would be:
 * <pre class="prettyprint">
 * &lt;updateRequestProcessorChain name="tolerant-chain"&gt;
 *   &lt;processor class="solr.TolerantUpdateProcessorFactory"&gt;
 *     &lt;int name="maxErrors"&gt;10&lt;/int&gt;
 *   &lt;/processor&gt;
 *   &lt;processor class="solr.RunUpdateProcessorFactory" /&gt;
 * &lt;/updateRequestProcessorChain&gt;
 * 
 * </pre>
 * 
 * <p>
 * The maxErrors parameter can be overwritten per request, for example:
 * </p>
 * <pre class="prettyprint">
 * curl http://localhost:8983/update?maxErrors=100 -H "Content-Type: text/xml" -d @myfile.xml
 * </pre>
 * 
 * 
 * 
 */
public class TolerantUpdateProcessorFactory extends UpdateRequestProcessorFactory
    implements UpdateRequestProcessorFactory.RunAlways {

  // nocommit: make SolrCoreAware and fail fast if no uniqueKey configured
  
  /**
   * Parameter that defines how many errors the UpdateRequestProcessor will tolerate
   */
  private final static String MAX_ERRORS_PARAM = "maxErrors";
  
  /**
   * Default maxErrors value that will be use if the value is not set in configuration
   * or in the request
   */
  private Integer defaultMaxErrors = Integer.MAX_VALUE;
  
  @SuppressWarnings("rawtypes")
  @Override
  public void init( NamedList args ) {

    // nocommit: clean error on invalid type for param ... don't fail stupidly on <str ...>42</str>
    Object maxErrorsObj = args.get(MAX_ERRORS_PARAM); 
    if (maxErrorsObj != null) {
      try {
        defaultMaxErrors = (Integer)maxErrorsObj;
      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Unnable to parse maxErrors parameter: " + maxErrorsObj, e);
      }
    }
  }
  
  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {

    // short circut if we're a replica processing commands from our leader
    DistribPhase distribPhase = DistribPhase.parseParam(req.getParams().get(DISTRIB_UPDATE_PARAM));
    if (DistribPhase.FROMLEADER.equals(distribPhase)) {
      return next;
    }
    
    Integer maxErrors = req.getParams().getInt(MAX_ERRORS_PARAM);
    if(maxErrors == null) {
      maxErrors = this.defaultMaxErrors;
    }

    // nocommit: support maxErrors < 0 to mean the same as Integer.MAX_VALUE (add test)
    
    // NOTE: even if 0==maxErrors, we still inject processor into chain so respones has expected header info
    return new TolerantUpdateProcessor(req, rsp, next, maxErrors, distribPhase);
  }
}
