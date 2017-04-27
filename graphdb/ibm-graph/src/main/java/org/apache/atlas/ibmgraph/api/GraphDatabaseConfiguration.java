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

package org.apache.atlas.ibmgraph.api;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Map;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.ibmgraph.MultiTenancyDisabledStrategy;
import org.apache.atlas.ibmgraph.PartitionPerTenantStrategy;
import org.apache.atlas.ibmgraph.TenantGraphStrategy;
import org.apache.atlas.ibmgraph.util.FileUtils;
import org.apache.commons.configuration.Configuration;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Options for creating a connection to IBM Graph.
 *
 */
public class GraphDatabaseConfiguration  {


    public static final int DEFAULT_ELEMENT_CACHE_EVICTION_WARNING_THROTTLE = 0;
    public static final int DEFAULT_ELEMENT_CACHE_CAPACITY = 10000;


    private static final String ATLAS_GRAPHDB_HTTP_RETRY_COUNT = "atlas.graphdb.httpMaxRetries";
    private static final int DEFAULT_HTTP_RETRY_COUNT = 10;

    private static final String ATLAS_GRAPHDB_SCHEMA_HTTP_RETRY_COUNT = "atlas.graphdb.schema.httpMaxRetries";
    private static final int DEFAULT_SCHEMA_HTTP_RETRY_COUNT = 30;

    //Use a relatively small schema size by default to minimize the chance of the index
    //creation failing with a BackendException
    private static final String ATLAS_GRAPHDB_SCHEMA_BATCH_SIZE = "atlas.graphdb.schema.batchSize";
    private static final int DEFAULT_SCHEMA_BATCH_SIZE=20;

    private static final String ATLAS_HTTP_CONNECTION_POOL_SIZE = "atlas.graphdb.httpConnectionPoolSize";
    private static final int DEFAULT_HTTP_CONNECTION_POOL_SIZE=200;

    private static final String ATLAS_HTTP_IDLE_TIMEOUT = "atlas.graphdb.httpIdleTimeoutMs";
    private static final int DEFAULT_HTTP_IDLE_TIMEOUT=30000;

    private static final String ATLAS_CONNECTION_REQUEST_TIMEOUT = "atlas.graphdb.httpConnectionRequestTimeoutMs";
    private static final int DEFAULT_CONNECTION_REQUEST_TIMEOUT = -1;

    private static final String ATLAS_CONNECTION_TIMEOUT = "atlas.graphdb.httpConnectionTimeoutMs";
    private static final int DEFAULT_CONNECTION_TIMEOUT = -1;

    private static final String ATLAS_SOCKET_TIMEOUT = "atlas.graphdb.httpSocketTimeoutMs";

    //1 hour.  This value needs to be large to account for potentially very long amounts of
    //time needed to create the indices.
    private static final int DEFAULT_SOCKET_TIMEOUT = 3600000;

    public static final String ATLAS_TENANT_GRAPH_STRATEGY_IMPL = "atlas.graphdb.tenantGraphStrategyImpl";
    private static final Class DEFAULT_TENANT_GRAPH_STRATEGY_IMPL = MultiTenancyDisabledStrategy.class;

    private static final Logger logger_ = LoggerFactory.getLogger(GraphDatabaseConfiguration.class);

    public static final GraphDatabaseConfiguration INSTANCE = new GraphDatabaseConfiguration();

    private TenantGraphStrategy strategy;

    private String username_;
    private String password_;
    private String baseUrl_;
    private String graphName_;

    private int maxRetries_ = 180; // retry for 15 minutes

    private long retryInterval_ = 5000; // 5 seconds by default

    public String getConnectionUsername() {
        return  username_;
    }

    public String getConnectionPassword() {
        return password_;
    }

    public String getBaseUrl() {
        return baseUrl_;
    }

    private GraphDatabaseConfiguration() {

        try {
            strategy = (TenantGraphStrategy)getTenantGraphStrategyClass().newInstance();
            logger_.info("Using tenant graph strategy: " + strategy.getClass().getName());
        }
        catch(Exception e) {
            logger_.error("Could not create tenant graph strategy!", e);
        }
        try {
            if(readCredentialsFromFile()) {
                return;
            }

            if(readCredentialsFromEnvironment()) {
                return;
            }
        }
        catch(JSONException e) {
            logger_.error("Could not read credentials.", e);
        } catch (IOException e) {
            logger_.error("Could not read credentials file.", e);
        }

    }

    private boolean readCredentialsFromEnvironment() throws JSONException {
        Map envs = System.getenv();
        if (envs.get("VCAP_APPLICATION") != null) {
            // app running in Bluemix
            if (envs.get("VCAP_SERVICES") != null) {
                // get the services bound to the app
                String graphServiceName = "GraphDataStore";
                String json = envs.get("VCAP_SERVICES").toString();
                JSONObject vcapSvcs = new JSONObject( json );
                if (!vcapSvcs.isNull(graphServiceName)) {
                    JSONObject credentials = vcapSvcs.getJSONArray(graphServiceName)
                            .getJSONObject(0);
                    readCredentials(credentials);
                    return true;

                }
            }
        }
        return false;
    }

    private boolean readCredentialsFromFile() throws IOException, JSONException {

        String configuredFile = System.getProperty("ibmgraph.credentials");
        URL u = null;
        if(configuredFile != null) {
            File f = new File(configuredFile);
            if(f.exists()) {
                u = f.toURI().toURL();
            }
        }

        if(u == null) {
            String atlasConfDirectoryName = System.getProperty("atlas.conf");
            if(atlasConfDirectoryName != null) {
                File atlasConfDirectory = new File(atlasConfDirectoryName);
                File credentialsFile = new File(atlasConfDirectory,"credentials.json");
                if(credentialsFile.exists()) {
                    u = credentialsFile.toURI().toURL();
                }
                else {
                    logger_.warn("A credentials.json file was not found in the configured atlas.conf directory: " + atlasConfDirectoryName);
                }
            }
        }

        if(u == null) {
            u = Thread.currentThread().getContextClassLoader().getResource("credentials.json");
        }

        if(u != null) {
            logger_.info("Reading IBM Graph credentials from: " + u.toString());
            InputStream is = u.openStream();
            String s = FileUtils.readStream(new InputStreamReader(is));
            is.close();
            JSONObject json = new JSONObject(s);
            readCredentials(json);
            return true;
        }
        else{
            logger_.error("Credentials file not found!");
        }
        return false;
    }

    private void readCredentials(JSONObject credentials) throws JSONException {
        JSONObject creds = credentials.getJSONObject("credentials");
        // get the URL for the GraphDB service
        baseUrl_ = creds.getString("apiURL");
        graphName_ = creds.getString("graphName");
        username_ = creds.getString("username");
        password_ = creds.getString("password");
    }



    public int getMaxRetries() {
        return maxRetries_;
    }

    public void setMaxRetries(int atlasMaxRetries) {
        maxRetries_ = atlasMaxRetries;
    }

    public long getRetryInterval() {
        return retryInterval_;
    }

    public void setRetryInterval(long atlasRetryInterval) {
        retryInterval_ = atlasRetryInterval;
    }

    public String getGraphName() {
        return graphName_;
    }

    public void setGraphName(String name) {
        graphName_ = name;
    }

    /**
     * Gets the batch size to use when calling the schema api.
     *
     */
    public int getSchemaCreationBatchSize() {
        try {
            return ApplicationProperties.get().getInteger(ATLAS_GRAPHDB_SCHEMA_BATCH_SIZE, DEFAULT_SCHEMA_BATCH_SIZE);
        }
        catch(AtlasException e) {
            logger_.error("Could not determine schema batch size.  Defaulting to " + DEFAULT_SCHEMA_BATCH_SIZE);
            return DEFAULT_SCHEMA_BATCH_SIZE;
        }

    }

    /**
     * Gets the size of the http connection pool to use.
     *
     * @return
     */
    public int getHttpConnectionPoolSize() {
        try {
            return ApplicationProperties.get().getInteger(ATLAS_HTTP_CONNECTION_POOL_SIZE, DEFAULT_HTTP_CONNECTION_POOL_SIZE);
        }
        catch(AtlasException e) {
            logger_.error("Could not determine connection pool size.  Defaulting to " + DEFAULT_HTTP_CONNECTION_POOL_SIZE);
            return DEFAULT_HTTP_CONNECTION_POOL_SIZE;
        }

    }

    /**
     * Gets the http idle timeout, in seconds.  Http connections that have been idle longer
     * than this amount of time are automatically closed.  If the timeout is less than 0, automatic idle
     * connection closing is disabled.
     *
     * @return
     */
    public int getHttpIdleTimeoutMs() {
        try {
            return ApplicationProperties.get().getInteger(ATLAS_HTTP_IDLE_TIMEOUT, DEFAULT_HTTP_IDLE_TIMEOUT);
        }
        catch(AtlasException e) {
            logger_.error("Could not determine http idle timeous.  Defaulting to " + DEFAULT_HTTP_IDLE_TIMEOUT);
            return DEFAULT_HTTP_IDLE_TIMEOUT;
        }
    }

    /**
     * Defines the socket timeout (<code>SO_TIMEOUT</code>) in milliseconds,
     * which is the timeout for waiting for data from IBM Graph, or, put differently,
     * a maximum period inactivity between two consecutive data packets).
     * <p/>
     * A timeout value of zero is interpreted as an infinite timeout.
     * A negative value is interpreted as undefined (system default).
     * <p/>
     */
    public int getHttpSocketTimeoutMs() {
        try {
            return ApplicationProperties.get().getInteger(ATLAS_SOCKET_TIMEOUT, DEFAULT_SOCKET_TIMEOUT);
        }
        catch(AtlasException e) {
            logger_.error("Could not determine http socket timeout.  Defaulting to " + DEFAULT_SOCKET_TIMEOUT);
            return DEFAULT_SOCKET_TIMEOUT;
        }
    }

    /**
     * Determines the timeout in milliseconds until a connection is established to IBM Graph.
     * A timeout value of zero is interpreted as an infinite timeout.
     * <p/>
     * A timeout value of zero is interpreted as an infinite timeout.
     * A negative value is interpreted as undefined (system default).
     * <p/>
     */
    public int getHttpConnectionTimeoutMs() {
        try {
            return ApplicationProperties.get().getInteger(ATLAS_CONNECTION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT);
        }
        catch(AtlasException e) {
            logger_.error("Could not determine http connection timeout.  Defaulting to " + DEFAULT_CONNECTION_TIMEOUT);
            return DEFAULT_CONNECTION_TIMEOUT;
        }
    }

    /**
     * Returns the timeout in milliseconds used when requesting a connection to IBM Graph.
     * A timeout value of zero is interpreted as an infinite timeout.
     * <p/>
     * A timeout value of zero is interpreted as an infinite timeout.
     * A negative value is interpreted as undefined (system default).
     * <p/>
     */
    public int getHttpConnectionRequestTimeoutMs() {
        try {
            return ApplicationProperties.get().getInteger(ATLAS_CONNECTION_REQUEST_TIMEOUT, DEFAULT_CONNECTION_REQUEST_TIMEOUT);
        }
        catch(AtlasException e) {
            logger_.error("Could not determine http connection request timeout.  Defaulting to " + DEFAULT_CONNECTION_REQUEST_TIMEOUT);
            return DEFAULT_CONNECTION_REQUEST_TIMEOUT;
        }
    }

    /**
     * Gets the TenantGraphStrategy implementation class.
     *
     * @return
     */
    public static Class getTenantGraphStrategyClass() {
        try {
            Configuration config = ApplicationProperties.get();
            return ApplicationProperties.getClass(config, ATLAS_TENANT_GRAPH_STRATEGY_IMPL, DEFAULT_TENANT_GRAPH_STRATEGY_IMPL.getName(), TenantGraphStrategy.class);
        }
        catch(AtlasException e) {
            logger_.error("Unable to use configured tenant graph strategy.  Defaulting to " + DEFAULT_TENANT_GRAPH_STRATEGY_IMPL.getName(), e);
            return DEFAULT_TENANT_GRAPH_STRATEGY_IMPL;
        }
    }

    public TenantGraphStrategy getTenantGraphStrategy() {
        return strategy;
    }

    public int getCreateSchemaHttpMaxRetries() {
        try {
            return ApplicationProperties.get().getInteger(ATLAS_GRAPHDB_SCHEMA_HTTP_RETRY_COUNT, DEFAULT_SCHEMA_HTTP_RETRY_COUNT);
        }
        catch(AtlasException e) {
            logger_.error("Could not determine http schema max retries.  Defaulting to " + DEFAULT_SCHEMA_HTTP_RETRY_COUNT);
            return DEFAULT_SCHEMA_HTTP_RETRY_COUNT;
        }
    }

    public int getHttpMaxRetries() {
        try {
            return ApplicationProperties.get().getInteger(ATLAS_GRAPHDB_HTTP_RETRY_COUNT, DEFAULT_HTTP_RETRY_COUNT);
        }
        catch(AtlasException e) {
            logger_.error("Could not determine http schema max retries.  Defaulting to " + DEFAULT_HTTP_RETRY_COUNT);
            return DEFAULT_HTTP_RETRY_COUNT;
        }
    }


    public static final String ELEMENT_CACHE_CAPACITY = "atlas.graphdb.ElementCache.capacity";

    /**
     * Get the configuration property that specifies the size of the compiled query
     * cache. This is an optional property. A default is used if it is not
     * present.
     *
     * @return the size to be used when creating the compiled query cache.
     */
    public static int getElementCacheCapacity() {
        try {
            return ApplicationProperties.get().getInt(ELEMENT_CACHE_CAPACITY, DEFAULT_ELEMENT_CACHE_CAPACITY);
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
    }

    public static final String ELEMENT_CACHE_EVICTION_WARNING_THROTTLE = "atlas.graphdb.ElementCache.evictionWarningThrottle";

    /**
     * Get the configuration property that specifies the number evictions that pass
     * before a warning is logged. This is an optional property. A default is
     * used if it is not present.
     *
     * @return the number of evictions before a warning is logged.
     */
    public static int getElementCacheEvictionWarningThrottle() {
        try {
            return ApplicationProperties.get().getInt(ELEMENT_CACHE_EVICTION_WARNING_THROTTLE, DEFAULT_ELEMENT_CACHE_EVICTION_WARNING_THROTTLE);
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
    };
}