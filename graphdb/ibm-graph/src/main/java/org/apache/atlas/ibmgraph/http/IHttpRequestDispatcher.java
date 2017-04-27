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

import org.apache.atlas.ibmgraph.exception.GraphDatabaseException;

/**
 * Abstraction for interacting with a HTTP server.
 */
public interface IHttpRequestDispatcher {

    /**
     * Dispatches a request to a http server.
     *
     * @param request
     * @throws GraphDatabaseException
     */
    HttpResponse dispatchRequest(HttpRequest request, String userNameToken)
            throws GraphDatabaseException;

    /**
     * Resets the state of the dispatcher. Forces underlying httpclient to be
     * recreated.
     */
    void reset();

    /**
     * @param request
     * @param userNameToken
     * @param retryStrategy - retry strategy to use.  If null, the default strategy ({@link HttpRetryStrategy}) will be used.
     * @return
     * @throws GraphDatabaseException
     */
    HttpResponse dispatchRequest(HttpRequest request, String userNameToken, IHttpRetryStrategy retryStrategy)
            throws GraphDatabaseException;
}
