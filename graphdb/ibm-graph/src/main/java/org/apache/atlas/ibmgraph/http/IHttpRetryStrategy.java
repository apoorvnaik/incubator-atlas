/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.ibmgraph.http;

import java.io.IOException;

import org.apache.http.protocol.HttpContext;
import org.apache.http.HttpResponse;

/**
 * Controls under the retry behavior when we execute a http request
 */
public interface IHttpRetryStrategy {

    /**
     * Determines whether a retry should be performed when the http request
     * fails with an IOException.
     *
     * @param exception
     * @param executionCount
     * @param context
     * @return whether or not to retry the request
     */
    boolean retryRequest(IOException exception, int executionCount, HttpContext context);

    /**
     * Determines whether a retry should be performed based on the response we
     * get back from the http server.
     *
     * @param response
     * @param executionCount
     * @param context
     * @return whether to retry the request
     */
    boolean retryRequest(HttpResponse response, int executionCount, HttpContext context);

}