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

import java.net.URI;

import org.apache.atlas.ibmgraph.api.GraphDatabaseConfiguration;
import org.apache.atlas.ibmgraph.api.json.JsonErrorResponse;
import org.apache.atlas.ibmgraph.exception.BadRequestException;
import org.apache.atlas.ibmgraph.exception.GraphDatabaseException;
import org.apache.atlas.ibmgraph.exception.HttpException;
import org.apache.atlas.ibmgraph.exception.InternalServerException;
import org.apache.atlas.ibmgraph.exception.NotFoundException;
import org.apache.atlas.ibmgraph.util.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

/**
 * Handles http requests
 */
public class HttpRequestHandler {

    private String uriString_ = GraphDatabaseConfiguration.INSTANCE.getBaseUrl();

    private static final String CLASS_NAME = HttpRequestHandler.class.getName();
    private static final Logger logger_ = LoggerFactory.getLogger(CLASS_NAME);

    protected IHttpRequestDispatcher ibmGraphRequestDispatcher_ = null;

    private static final Object sessionLock = new Object();

    private static volatile String userNameToken;

    @SuppressWarnings("unused")
    public HttpRequestHandler(IHttpRequestDispatcher dispatcher) {
        //for unit testing, allows graph database to be mocked out
        ibmGraphRequestDispatcher_ = dispatcher;
        // force the subclasses to not use the default constructor
    }

    public HttpRequestHandler() {
        ibmGraphRequestDispatcher_ =  new HttpRequestDispatcher();

    }

    private JsonElement processSessionRequest(RequestType requestType, String path,
            String json)
                    throws GraphDatabaseException {

        logger_.info("Creating New session token " + path + ": " + json);
        // translate the metadata request objct to the IBM Graph http request
        HttpRequest ibmGraphRequest = translateRequest(requestType, path, json);

        // dispatch the request to IBM Graph
        HttpResponse ibmGraphResponse = dispatchRequest(ibmGraphRequest, null, null);

        // translate the IBM Graph response
        return translateResponse(ibmGraphRequest, json, ibmGraphResponse);
    }


    public JsonElement processRequest(RequestType requestType,
                                      String path, String json) {
        return processRequest(requestType, path, json, null);
    }

    public JsonElement processRequest(RequestType requestType,
            String path, String json, IHttpRetryStrategy retryStrategy) {
        if (userNameToken == null) {
            if (logger_.isDebugEnabled()) {
                logger_.debug(" Waiting for session create request to complete");
            }
            synchronized (sessionLock) {
                if (userNameToken == null) { //recheck if username token is still null and has not been initialized by another thread.
                    // OMS-279 Multiple threads trying to create session. Runs into too many requests issues.
                    initializeSession();
                } else {
                    if (logger_.isDebugEnabled()) {
                        logger_.debug(" Session does not need to be created any more.");
                    }
                }
            }
        }

        // translate the request object to the IBM Graph http request
        HttpRequest ibmGraphRequest = translateRequest(requestType, path, json);
        try {
            return processRequest_(requestType, json, ibmGraphRequest, retryStrategy);
        } catch (IllegalStateException e) {
            //If the http client has been idle for too long, attempts to dispatch http requests
            //fail with an IllegalState exception.  When that happens, create a new
            //http client and try again.
            logger_.warn("HttpClient got into an unexpected state: " + e.getMessage() + ".  Retrying.");
            ibmGraphRequestDispatcher_.reset();
            return processRequest_(requestType, json, ibmGraphRequest, retryStrategy);
        }
    }

    private JsonElement processRequest_(RequestType requestType, String json,
                                                       HttpRequest ibmGraphRequest, IHttpRetryStrategy retryStrategy) {

        // dispatch the request to IBM Graph
        HttpResponse ibmGraphResponse = dispatchRequest(ibmGraphRequest, userNameToken, retryStrategy);

        if (ibmGraphResponse.getStatusCode() == HttpCode.FORBIDDEN
                || ibmGraphResponse.getStatusCode() == HttpCode.AUTHENTICATION_ERROR) {
            String expiredUserNameToken = userNameToken;
            //If we get forbidden error then create new session token and try again.
            synchronized(sessionLock) {
                if( userNameToken == expiredUserNameToken) {
                    initializeSession();
                }
            }

            // dispatch the request to IBM Graph
            ibmGraphResponse = dispatchRequest(ibmGraphRequest, userNameToken, retryStrategy);
        }

        // translate the IBM Graph response
        return translateResponse(ibmGraphRequest, json, ibmGraphResponse);

    }

    //Currently session token is static for static credentials. If we make credentials dynamic, we will need to associate the token with credentials.

    private void initializeSession() throws GraphDatabaseException {
            if (logger_.isDebugEnabled()) {
                logger_.debug(" session token is NOT valid. Creating new token. Current session token is {}", userNameToken);
            }
            JsonElement response = processSessionRequest(RequestType.GET,
                    Endpoint.SESSION.getUrl(null), null);
            userNameToken = "gds-token "+response.getAsJsonObject().get("gds-token").getAsString();
        }

    protected HttpRequest translateRequest(RequestType requestType, String path,
            String json) throws GraphDatabaseException {

        HttpRequest ibmGraphRequest = new HttpRequest();
        ibmGraphRequest.setRequestType(requestType);

        URI uri = HttpUtils.getApiUri(uriString_, path);
        ibmGraphRequest.setUri(uri);

        ibmGraphRequest.setJson(json);
        return ibmGraphRequest;

    }

    /**
     * Dispatches/submits the REST request to graph db
     *
     * @param omRequest
     * @param retryStrategy - retry strategy to use.  If null, the default strategy ({@link HttpRetryStrategy}) will be used.
     * @return
     * @throws GraphDatabaseException
     */
    protected HttpResponse dispatchRequest(HttpRequest omRequest, String sessionToken, IHttpRetryStrategy retryStrategy) throws GraphDatabaseException {

        HttpResponse httpResponse = ibmGraphRequestDispatcher_.dispatchRequest(omRequest, sessionToken, retryStrategy);


        return httpResponse;
    }

    /**
     * Subclasses should implement this to translate request to
     * {@link org.apache.atlas.ibmGraph.client.impl.HttpResponse}
     *
     * @param ibmGraphResponse
     * @param responseClass
     * @return
     * @throws GraphDatabaseException
     */
    protected JsonElement translateResponse(HttpRequest request, String requestJson, HttpResponse ibmGraphResponse)
            throws GraphDatabaseException {

        // handle failed request
        if (!isSuccess(ibmGraphResponse)) {
            handleFailures(request, ibmGraphResponse);
            return null; // should never reach here
        }

        String json = ibmGraphResponse.getResponseBody();
        if(json == null) {
            return null;
        }

        JsonParser parser = new JsonParser();
        return parser.parse(json);
    }

    private boolean isSuccess(HttpResponse response) {
        int statusCode = response.getStatusCode();
        return statusCode == HttpCode.OK || statusCode == HttpCode.CREATED || statusCode == HttpCode.ACCEPTED;
    }


    /**
     * @param ibmGraphResponse
     * @return The error message in the IBM Graph http response
     */
    protected String getErrorMessage(HttpResponse ibmGraphResponse) {

        logger_.error("Processing error: " + ibmGraphResponse.getResponseBody());
        JsonErrorResponse error = new Gson().fromJson(ibmGraphResponse.getResponseBody(),JsonErrorResponse.class);
        if(error != null) {
            return error.getMessage();
        }

        String errorMessage = "HTTP " + ibmGraphResponse.getStatusCode() + ": "
                + ibmGraphResponse.getReasonPhrase();
        return errorMessage;
    }

    /**
     * Throws an exception that corresponds to the error code in the response
     *
     * @param ibmGraphResponse the response to process
     * @throws GraphDatabaseException corresponding to the error in the response
     */
    protected void handleFailures(HttpRequest request, HttpResponse ibmGraphResponse) throws GraphDatabaseException {


        if (ibmGraphResponse.getStatusCode() == HttpCode.INTERNAL_SERVER_ERROR) {

            throw new InternalServerException(request, ibmGraphResponse);
        }
        if (ibmGraphResponse.getStatusCode() == HttpCode.BAD_REQUEST) {

            throw new BadRequestException(request, ibmGraphResponse);
        }

        if (ibmGraphResponse.getStatusCode() == HttpCode.NOT_FOUND) {

            throw new NotFoundException(request, ibmGraphResponse);
        }


        throw new HttpException(request, ibmGraphResponse);
    }


    public void setOmsHttpRequestDispatcher(IHttpRequestDispatcher dispatcher) {
        ibmGraphRequestDispatcher_ = dispatcher;
    }
}
