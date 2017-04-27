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
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.atlas.ibmgraph.api.GraphDatabaseConfiguration;
import org.apache.atlas.ibmgraph.exception.GraphDatabaseException;
import org.apache.atlas.ibmgraph.util.GraphDBUtil;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultUserTokenHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpRequestDispatcher implements IHttpRequestDispatcher, HttpRequestRetryHandler, ServiceUnavailableRetryStrategy {

    /**
     *
     */
    private static final String RETRY_STRATEGY_CONTEXT_ATTRIBUTE = "retryStrategy";
    private static final String CLASS_NAME = HttpRequestDispatcher.class.getName();
    private static final Logger logger_ = LoggerFactory.getLogger(CLASS_NAME);
    private static CloseableHttpClient cachedClient_;
    private static IdleConnectionMonitorThread monitorThread_;

    private static final RequestConfig REQUEST_CONFIG = RequestConfig.
            custom()
            .setConnectTimeout(GraphDatabaseConfiguration.INSTANCE.getHttpConnectionTimeoutMs())
            .setConnectionRequestTimeout(GraphDatabaseConfiguration.INSTANCE.getHttpConnectionRequestTimeoutMs())
            .setSocketTimeout(GraphDatabaseConfiguration.INSTANCE.getHttpSocketTimeoutMs())
            .build();
    @Override
    public HttpResponse dispatchRequest(HttpRequest request, String userNameToken) throws GraphDatabaseException {
        return dispatchRequest(request, userNameToken, null);

    }
    @Override
    public HttpResponse dispatchRequest(HttpRequest request, String userNameToken, IHttpRetryStrategy retryStrategy) throws GraphDatabaseException {
        final String methodName = "dispatchRequest";
        logger_.debug("Entering " + CLASS_NAME + "." + methodName + ", request: " + request.toString());
        CloseableHttpClient client = getHttpClient(userNameToken);
        HttpResponse result = null;
        try {

            boolean usePreemptiveAuth = userNameToken == null;
            CloseableHttpResponse response = executeRequest(request, client, usePreemptiveAuth, retryStrategy);
            result = processResponse(response);
        } catch (IOException e) {
            logger_.error("Could not process http response", e);
            throw new GraphDatabaseException("Could not process http response", e);
        } finally {
            try {
                if (userNameToken == null) {// if client is for creating session then close it.
                    client.close();
                }
            } catch (IOException e) {
                logger_.error("Error shutting down httpclient", e);
            }
        }

        logger_.debug("Exiting " + CLASS_NAME + "." + methodName + ", result: " + result.toString());
        return result;
    }

    private CloseableHttpClient getHttpClient(String userNameToken) throws GraphDatabaseException {

        CloseableHttpClient client = cachedClient_;
        if (userNameToken != null ) {

            if (client == null){

                PoolingHttpClientConnectionManager cm = createConnectionManager();
                HttpClientBuilder builder = createBuilder(cm);

                builder.setUserTokenHandler(DefaultUserTokenHandler.INSTANCE);
                List<BasicHeader> defaultHeaders = new ArrayList<BasicHeader>();
                defaultHeaders.add(new BasicHeader("Authorization",
                        userNameToken));
                builder.setDefaultHeaders(defaultHeaders);

                client = cachedClient_= builder.build();

                if(monitorThread_ != null) {
                    monitorThread_.shutdown();
                }
                monitorThread_ = new IdleConnectionMonitorThread(cm);
                monitorThread_.start();
            }
        }
        else
        {

            HttpClientBuilder builder = createBuilder(null);

            try {
                if (cachedClient_!= null) {
                    cachedClient_.close();//close the previously cached client.
                }
            } catch (IOException e) {
                logger_.info("Closing stale connection from previous session" );
            }
            CredentialsProvider credentialsProvider = createCredentialsProvider();
            if (credentialsProvider != null) {
                builder.setDefaultCredentialsProvider(credentialsProvider);
            }
            client = builder.build();
        }

        return client;
    }
    private HttpClientBuilder createBuilder(PoolingHttpClientConnectionManager cm) {
        HttpClientBuilder builder = HttpClientBuilder.create();
        builder.setRetryHandler(this);
        builder.setServiceUnavailableRetryStrategy(this);
        if(cm != null) {
            builder.setConnectionManager(cm);
        }
        return builder;
    }
    private PoolingHttpClientConnectionManager createConnectionManager() {
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        // Set max total connections (default is 2)
        cm.setMaxTotal(GraphDatabaseConfiguration.INSTANCE.getHttpConnectionPoolSize());
        // we only use one route.
        cm.setDefaultMaxPerRoute(GraphDatabaseConfiguration.INSTANCE.getHttpConnectionPoolSize());
        return cm;
    }

    private CredentialsProvider createCredentialsProvider() throws GraphDatabaseException {
        String user = GraphDatabaseConfiguration.INSTANCE.getConnectionUsername();
        String password = GraphDatabaseConfiguration.INSTANCE.getConnectionPassword();
        if(user == null) {
            return null;
        }
        CredentialsProvider provider = new BasicCredentialsProvider();

        provider.setCredentials(
                AuthScope.ANY,
                new UsernamePasswordCredentials(user, password));
        return provider;
    }

    private CloseableHttpResponse executeRequest(HttpRequest request,
            CloseableHttpClient client, boolean usePreemptiveAuth, IHttpRetryStrategy retryStrategy) throws GraphDatabaseException {

        if (request.getRequestType() == RequestType.POST) {
            return executePostRequest(client, request, usePreemptiveAuth, retryStrategy);
        }

        if (request.getRequestType() == RequestType.GET) {
            return executeGetRequest(client, request, usePreemptiveAuth, retryStrategy);
        }

        if (request.getRequestType() == RequestType.PUT) {
            return executePutRequest(client, request, usePreemptiveAuth, retryStrategy);
        }

        if (request.getRequestType() == RequestType.DELETE) {
            return executeDeleteRequest(client, request, usePreemptiveAuth, retryStrategy);
        }

        return null;
    }

    private CloseableHttpResponse executeGetRequest(CloseableHttpClient client,
            HttpRequest request, boolean usePreemptiveAuth, IHttpRetryStrategy retryStrategy) throws GraphDatabaseException {

        try {
            HttpGet get = new HttpGet(request.getUri());
            return executeRequest_(client, request, get, usePreemptiveAuth, retryStrategy);

        } catch (ClientProtocolException e) {
            logger_.error("Could not send GET request to IBM Graph. "
                    + request.getUri().toString(), e);
            throw new GraphDatabaseException("Could not send GET request to IBM Graph. "
                    + request.getUri().toString(), e);
        } catch (IOException e) {
            logger_.error("Could not send GET request to IBM Graph: "
                    + request.getUri().toString(), e);
            throw new GraphDatabaseException("Could not send GET request to IBM Graph. "
                    + request.getUri().toString(), e);
        }
    }

    private CloseableHttpResponse executeRequest_(CloseableHttpClient client,
            HttpRequest request, HttpRequestBase httpRequest,  boolean usePreemptiveAuth, IHttpRetryStrategy retryStrategy) throws IOException, ClientProtocolException {

        HttpClientContext ctx = HttpClientContext.create();
        httpRequest.setConfig(REQUEST_CONFIG);
        if(retryStrategy != null) {
            ctx.setAttribute(RETRY_STRATEGY_CONTEXT_ATTRIBUTE, retryStrategy);
        }
        if(! usePreemptiveAuth) {
            return client.execute(httpRequest, ctx);
        }

        //set up preemptive authentication
        URI uri = request.getUri();
        String hostname = uri.getHost();
        int port = uri.getPort();
        String scheme = uri.getScheme();
        HttpHost target = new HttpHost(hostname, port, scheme);

        AuthCache cache = new BasicAuthCache();
        BasicScheme basicAuth = new BasicScheme();
        cache.put(target, basicAuth);

        ctx.setAuthCache(cache);

        return client.execute(target, httpRequest, ctx);
    }

    private CloseableHttpResponse executePostRequest(CloseableHttpClient client,
            HttpRequest request, boolean usePreemptiveAuth, IHttpRetryStrategy retryStrategy) throws GraphDatabaseException {

        try {
            HttpPost post = new HttpPost(request.getUri());
            post.addHeader("Content-Type", "application/json");
            String json = request.getJson();
            if(json != null) {
                StringEntity entity = new StringEntity(json, ContentType.APPLICATION_JSON);
                post.setEntity(entity);
            }
            return executeRequest_(client, request, post, usePreemptiveAuth, retryStrategy);

        } catch (ClientProtocolException e) {
            logger_.error("Could not send POST request to IBM Graph: "
                    + request.getUri().toString(), e);
            throw new GraphDatabaseException("Could not send POST request to IBM Graph: "
                    + request.getUri().toString(), e);
        } catch (IOException e) {
            logger_.error("Could not send POST request to IBM Graph: "
                    + request.getUri().toString(), e);
            throw new GraphDatabaseException("Could not send POST request to IBM Graph: "
                    + request.getUri().toString(), e);
        }

    }

    private CloseableHttpResponse executePutRequest(CloseableHttpClient client,
            HttpRequest request, boolean usePreemptiveAuth, IHttpRetryStrategy retryStrategy) throws GraphDatabaseException {

        try {
            HttpPut put = new HttpPut(request.getUri());

            if (request.getJson() != null) {
                InputStreamEntity entity = new InputStreamEntity(new ByteArrayInputStream(
                        request.getJson().getBytes()), request.getJson().length(),
                        ContentType.APPLICATION_JSON);
                put.setEntity(entity);
            }
            return executeRequest_(client, request, put, usePreemptiveAuth, retryStrategy);

        } catch (ClientProtocolException e) {
            logger_.error("Could not send PUT request to IBM Graph: "
                    + request.getUri().toString(), e);
            throw new GraphDatabaseException("Could not send PUT request to IBM Graph: "
                    + request.getUri().toString(), e);
        } catch (IOException e) {
            logger_.error("Could not send PUT request to  IBM Graph: "
                    + request.getUri().toString(), e);
            throw new GraphDatabaseException("Could not send PUT request to  IBM Graph: "
                    + request.getUri().toString(), e);
        }
    }

    private CloseableHttpResponse executeDeleteRequest(CloseableHttpClient client,
            HttpRequest request, boolean usePreemptiveAuth, IHttpRetryStrategy retryStrategy) throws GraphDatabaseException {

        try {
            HttpDelete delete = new HttpDelete(request.getUri());
            return executeRequest_(client, request, delete, usePreemptiveAuth, retryStrategy);

        } catch (ClientProtocolException e) {
            logger_.error("Could not send GET request to  IBM Graph: "
                    + request.getUri().toString(), e);
            throw new GraphDatabaseException("Could not send GET request to  IBM Graph: "
                    + request.getUri().toString(), e);
        } catch (IOException e) {
            logger_.error("Could not send GET request to IBM Graph: "
                    + request.getUri().toString(), e);
            throw new GraphDatabaseException("Could not send GET request to  IBM Graph: "
                    + request.getUri().toString(), e);
        }
    }

    private HttpResponse processResponse(CloseableHttpResponse response) throws IOException {

        if (response == null) {
            return null;
        }

        try {
            HttpResponse result = new HttpResponse();
            result.setReasonPhrase(response.getStatusLine().getReasonPhrase());
            result.setStatusCode(response.getStatusLine().getStatusCode());
            if (response.getEntity() != null) {

                String responseBody = getResponseBody(response);
                result.setResponseBody(responseBody);
                Header header = response.getFirstHeader(GraphDBUtil.IBM_GRAPH_REQUEST_ID_HEADER);
                if(header != null) {
                    String requestId = header.getValue();
                    result.setRequestId(requestId);
                }

                HttpEntity entity = response.getEntity();
                if (entity.getContentType() != null) {
                    result.setContentType(entity.getContentType().getValue());
                }
            }
            return result;
        } finally {
            response.close();
        }
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

    @Override
    public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
        return getRetryStrategy(context).retryRequest(exception, executionCount, context);
    }

    private IHttpRetryStrategy getRetryStrategy(HttpContext ctx) {
        IHttpRetryStrategy value = (IHttpRetryStrategy)ctx.getAttribute(RETRY_STRATEGY_CONTEXT_ATTRIBUTE);
        if(value == null) {
            return HttpRetryStrategy.INSTANCE;
        }
        return value;
    }

    /* (non-Javadoc)
     * @see org.apache.http.client.ServiceUnavailableRetryStrategy#retryRequest(org.apache.http.HttpResponse, int, org.apache.http.protocol.HttpContext)
     */
    @Override
    public boolean retryRequest(org.apache.http.HttpResponse response, int executionCount, HttpContext context) {

        return getRetryStrategy(context).retryRequest(response, executionCount, context);
    }


    @Override
    public long getRetryInterval() {
        //hope that the error was transient.  Retry after 5 seconds.
        return 5000;
    }

    @Override
    public void reset() {
        if (cachedClient_ != null) {
            try {
                cachedClient_.close();
            } catch (IOException e) {
                logger_.warn("Could not close cached http client");
            }
        }
        cachedClient_ = null;

    }

}
