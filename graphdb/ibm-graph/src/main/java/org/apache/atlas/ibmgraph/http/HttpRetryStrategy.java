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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.atlas.ibmgraph.api.GraphDatabaseConfiguration;
import org.apache.http.HttpResponse;
/**
 * Default implementation of IHttpRetryStrategy.
 *
 */
public class HttpRetryStrategy implements IHttpRetryStrategy {

    private static final Logger logger_ = LoggerFactory.getLogger(HttpRetryStrategy.class);
    private static final int MAX_RETRIES = GraphDatabaseConfiguration.INSTANCE.getHttpMaxRetries();
    public static final HttpRetryStrategy INSTANCE = new HttpRetryStrategy();

    static class RetryableBadRequestError {

        private String name;
        private boolean addPause;

        public RetryableBadRequestError(String name) {
            this(name, false);
        }

        public RetryableBadRequestError(String name, boolean addPause) {
            super();
            this.name = name;
            this.addPause = addPause;
        }

        public String getName() {
            return name;
        }
        public boolean isAddPause() {
            return addPause;
        }
    }

    private static final RetryableBadRequestError[] RETRYABLE_BAD_REQUEST_ERRORS = {
            new RetryableBadRequestError("com.thinkaurelius.titan.diskstorage.PermanentBackendException", true),
            new RetryableBadRequestError("ProcessClusterEventTimeoutException"),
            new RetryableBadRequestError("com.thinkaurelius.titan.diskstorage.TemporaryBackendException"),
            new RetryableBadRequestError(TimeoutException.class.getName()),
            new RetryableBadRequestError("java.net.SocketTimeoutException"),
            new RetryableBadRequestError("UnavailableException", true),
            new RetryableBadRequestError("com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException", true),
            new RetryableBadRequestError("com.thinkaurelius.titan.diskstorage.locking.TemporaryLockingException", true),
            //So far we've only seen this when getting the index status, so putting this here for now.  We may need to revisit
            //this if we encounter the error while trying to actually create indices.
            new RetryableBadRequestError("java.lang.IllegalArgumentException: Schema vertex is not a type vertex", true),
            new RetryableBadRequestError(NullPointerException.class.getName(), true)
    };

    protected HttpRetryStrategy() {

    }

    @Override
    public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {

        if (!(exception instanceof HttpHostConnectException)) {
            return false;
        }

        if (executionCount > getMaxRetries()) {
            return false;
        }

        try {
            logger_.info("Retrying in 5 seconds...");
            Thread.sleep(getRetryInterval());
        } catch (InterruptedException e) {
            logger_.warn("Interrupted while waiting to retry connection to Atlas", e);
        }
        return true;
    }

    protected int getMaxRetries() {
        return MAX_RETRIES;
    }


    @Override
    public boolean retryRequest(HttpResponse response, int executionCount, HttpContext context) {

        if(executionCount > getMaxRetries()) {
            return false;
        }

        int code = response.getStatusLine().getStatusCode();
        if(code == HttpCode.TIME_OUT) {
            logResponseCausingRetry(response);
            logRetry("Encountered HTTP timeout.", executionCount);
            return true;
        }

        if(code == HttpCode.INTERNAL_SERVER_ERROR) {
            return isRetryInternalServerError(response, executionCount);
        }

        if(code == HttpCode.BAD_GATEWAY) {
            logResponseCausingRetry(response);
            logRetry("Encountered Bad Gateway Error.", executionCount);
            return true;
        }

        if(code == HttpCode.BAD_REQUEST) {

            String responseBody = safelyGetResponseBody(response);

            return isRetryBadRequest(response, executionCount, responseBody);
        }

        return false;

    }

    protected boolean isRetryBadRequest(HttpResponse response, int executionCount, String responseBody) {
        for(RetryableBadRequestError retryableError : RETRYABLE_BAD_REQUEST_ERRORS) {

            if(responseBody.contains(retryableError.getName())) {
                logResponseCausingRetry(response);
                logRetry("Encountered " + retryableError.getName() + ".", executionCount);
                if(retryableError.isAddPause()) {
                    try {
                        //sleep a random amount of time to space out the requests
                        Thread.sleep((long)(5000*Math.random()));
                    }
                    catch(InterruptedException e) {
                        //ok
                    }
                }
                return true;
            }
        }
        return false;
    }


    protected boolean isRetryInternalServerError(org.apache.http.HttpResponse response, int tryNumber) {
        logResponseCausingRetry(response);
        logRetry("Encountered Internal Server Error.", tryNumber);
        return true;
    }

    protected void logRetry(String message, int tryNumber) {
        logger_.warn(message + "  Retrying (try # " + tryNumber + ") ...");
    }

    protected void logResponseCausingRetry(org.apache.http.HttpResponse response) {
        logger_.debug("Retrying request because the following response was received: " + response + ".  Body: " + safelyGetResponseBody(response));
    }


    public long getRetryInterval() {
        //hope that the error was transient.  Retry after 5 seconds.
        return 5000;
    }

    protected String safelyGetResponseBody(org.apache.http.HttpResponse response) {
        String responseBody = getResponseBody(response);

        //restore http entity so it can be read again during the request processing
        response.setEntity(new InputStreamEntity(new ByteArrayInputStream(responseBody.getBytes())));
        return responseBody;
    }

    private String getResponseBody(org.apache.http.HttpResponse response) {
        try {
            ByteArrayOutputStream responseStream = new ByteArrayOutputStream();
            response.getEntity().writeTo(responseStream);
            String responseBody = new String(responseStream.toByteArray());
            return responseBody;
        }
        catch(IOException e) {
            logger_.warn("Could not read http response", e);
            return null;
        }
    }
}

