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

package org.apache.atlas.ibmgraph;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.atlas.AtlasException;
import org.apache.atlas.GraphInitializationException;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.ibmgraph.IBMGraphMetadata.GraphState;
import org.apache.atlas.ibmgraph.api.GraphDatabaseClient;
import org.apache.atlas.ibmgraph.api.GraphDatabaseConfiguration;
import org.apache.atlas.ibmgraph.gremlin.expr.TransformQueryResultExpression;
import org.apache.atlas.ibmgraph.gremlin.step.GremlinStep;
import org.apache.atlas.ibmgraph.http.IBMGraphRequestContext;
import org.apache.atlas.ibmgraph.util.PersistentType;
import org.apache.atlas.mt.ITenantRegistrationListener;
import org.apache.atlas.mt.MultiTenancyConstants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.GraphDatabase;
import org.apache.atlas.repository.graphdb.GremlinVersion;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of GraphDatabase that works with IBM Graph.
 */
public class IBMGraphDatabase implements GraphDatabase<IBMGraphVertex, IBMGraphEdge> {

    private static final String TEST_GRAPH_PROPERTY = "ibm.graph.test.graph.name";
    private static final String TEST_TENANT_PROPERTY = "ibm.graph.test.tenant.name";
    private static final String SKIP_TEST_CLEANUP_PROPERTY = "ibm.graph.test.graph.skip.cleanup";

    private static final Logger logger_ = LoggerFactory.getLogger(IBMGraphDatabase.class);

    private static final int DEFAULT_TENANT_CACHE_SIZE = 1000;

    /**
     * Map of tenant ID -> IBMGraphGraph for the tenant.
     */
    private static final Map<String, IBMGraphGraph> TENANT_GRAPH_MAP = new ConcurrentHashMap<>(DEFAULT_TENANT_CACHE_SIZE);

    /**
     * Map of graph name -> IBMGraphMetadata for the graph
     */
    private static final Map<String, IBMGraphMetadata> GRAPH_METADATA_MAP = new ConcurrentHashMap<>();
    private ITenantRegistrationListener listener;


    /**
     * Constructor.
     */
    public IBMGraphDatabase() {

    }

    private TenantGraphStrategy getTenantGraphStrategy() {
        return GraphDatabaseConfiguration.INSTANCE.getTenantGraphStrategy();
    }

    private IBMGraphMetadata initializeIfNeeded(String graphName) throws GraphInitializationException {

        IBMGraphMetadata md = getOrCreateMetadata(graphName);

        if(md.getState() != GraphState.INITIALIZED) {
            synchronized(md) {
                if(md.getState() == GraphState.UNINITIALIZED) {
                    if(getTenantGraphStrategy().isGraphInitializationNeeded()) {
                        if(listener != null) {
                            md.setState(GraphState.INITIALIZING);
                            try {
                                listener.register();
                            }
                            catch(AtlasException e) {
                                throw new GraphInitializationException("Tenant initialization failed", e);
                            }
                            md.setState(GraphState.INITIALIZED);
                        }
                    }
                    else {
                        md.setState(GraphState.INITIALIZED);
                    }
                }
            }
        }

        return md;
    }

    private IBMGraphMetadata getOrCreateMetadata(String graphName) throws GraphInitializationException {

        IBMGraphMetadata md = null;
        md = GRAPH_METADATA_MAP.get(graphName);
        if(md == null) {
            synchronized(GRAPH_METADATA_MAP) {
                //check if another thread added the graph to the map
                md = GRAPH_METADATA_MAP.get(graphName);
                if(md == null) {
                    md = new IBMGraphMetadata(graphName);
                    GRAPH_METADATA_MAP.put(graphName, md);
                }
            }
        }

        //Don't block all tenants while creating the graph and getting its schema.  Just block other threads trying
        //to use the same graph.  This allows graphs to be created in parallel.
        if(md.getState() == GraphState.UNSET) {
            synchronized(md) {
                //check if another thread already did this initialization
                if(md.getState() == GraphState.UNSET) {
                    GraphDatabaseClient client = new GraphDatabaseClient(graphName);
                    boolean isNewGraph = client.createGraph();

                    if( ! isNewGraph) {
                        IBMGraphManagement mgmt = new IBMGraphManagement(md, client, getTenantGraphStrategy().getSharedTenantId());

                        try {
                            md.updateSchemaCache(client, mgmt);
                        }
                        finally {
                            mgmt.rollback();
                        }
                        md.setState(GraphState.INITIALIZED);
                    }
                    else {
                        md.setState(GraphState.UNINITIALIZED);
                    }
                }
            }
        }
        return md;
    }


    private IBMGraphGraph getOrCreateGraph(IBMGraphMetadata md, String tenantId) throws GraphInitializationException {
        if(! TENANT_GRAPH_MAP.containsKey(tenantId)) {

            synchronized(TENANT_GRAPH_MAP) {
                //Check if another thread has already added a graph to the map
                if (! TENANT_GRAPH_MAP.containsKey(tenantId)) {
                    IBMGraphGraph graph = new IBMGraphGraph(this, md, tenantId);
                    cacheGraph(tenantId, graph);
                }
            }
        }
        return TENANT_GRAPH_MAP.get(tenantId);
    }



    private IBMGraphGraph getGraph(String tenantId) throws GraphInitializationException {

        String tenantIdToLookup = tenantId;
        if(tenantIdToLookup == null) {
            tenantIdToLookup = getTenantGraphStrategy().getSharedTenantId();
        }
        String graphId = getTenantGraphStrategy().getGraphName(tenantId);
        graphId = graphId.toLowerCase();
        IBMGraphMetadata metadata = initializeIfNeeded(graphId);
        IBMGraphGraph graph = getOrCreateGraph(metadata, tenantIdToLookup);
        return graph;
    }

    private static void cacheGraph(String tenantId, IBMGraphGraph graph) {
        TENANT_GRAPH_MAP.put(tenantId, graph);
    }


    @Override
    public boolean isGraphLoaded() {

        synchronized (TENANT_GRAPH_MAP) {
            return TENANT_GRAPH_MAP.containsKey(IBMGraphRequestContext.get().getTenantId());
        }
    }

    @Override
    public AtlasGraph<IBMGraphVertex, IBMGraphEdge> getUserGraph() throws GraphInitializationException {
        return getGraph(getTenantGraphStrategy().getUserTenantId());
    }

    @Override
    public AtlasGraph<IBMGraphVertex, IBMGraphEdge> getSharedGraph() throws GraphInitializationException {
        return getGraph(getTenantGraphStrategy().getSharedTenantId());
    }


    @Override
    public void initialize(Map<String, String> initParameters) throws GraphInitializationException {
        String tenantId = null;
        if (initParameters != null ) {
            tenantId = initParameters.get(MultiTenancyConstants.TENANT_ID_PARAMETER);
            logger_.debug(tenantId+": loading graph");
        }

        setTenantId(tenantId);

        getGraph(getTenantIdFromRequestContext());
    }

    /**
     * Sets the tenant id in IBMGraphRequestContext and RequestContext
     * @param tenantId
     */
    public static void setTenantId(String tenantId) {
        String toSet = tenantId;
        if(tenantId == null || tenantId.isEmpty()) {
            logger_.debug(Thread.currentThread().getName()+": Using default tenant id");
            toSet = GraphDatabaseConfiguration.INSTANCE.getTenantGraphStrategy().getSharedTenantId();
        }
        IBMGraphRequestContext.get().setTenantId(toSet);
        //also set the tenant id in the request context, so that the type store uses it.  This is needed when the default tenant id is being used.

        setTenantIdInRequestContext(toSet);


    }

    //TODO (for MT support) - this should be added to GraphDatabase
    //interface and called by AtlasGraphProvider
    public void registerListener(ITenantRegistrationListener listener) {
        this.listener = listener;
    }

    public ITenantRegistrationListener getListener() {
        return listener;
    }


    @Override
    public void initializeTestGraph() {
        String tenantId = null;
        if (System.getProperty(TEST_TENANT_PROPERTY) != null) {
            tenantId = System.getProperty(TEST_TENANT_PROPERTY);
        } else {
            tenantId = "test-" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();
        }
        String graphName = tenantId;
        String graphNameOverride = System.getProperty(TEST_GRAPH_PROPERTY);
        if(graphNameOverride != null) {
            graphName =  graphNameOverride;
        }

        //for POJO tests, we use a different graph for each test
        //class for efficiency.
        logger_.info("Setting graph name to {}", graphName);
        GraphDatabaseConfiguration.INSTANCE.setGraphName(graphName);


        logger_.info("Setting tenantId to {}", tenantId);
        setTenantId(tenantId);

        setTenantIdInRequestContext(tenantId);

    }

    @Override
    public void cleanup(boolean deleteGraph) {

        String skipCleanup = System.getProperty(SKIP_TEST_CLEANUP_PROPERTY);
        if (skipCleanup != null && Boolean.parseBoolean(skipCleanup)) {
            return;
        }
        String tenantId = getTenantIdFromRequestContext();
        String graphId = getTenantGraphStrategy().getGraphName(tenantId);
        if(! GRAPH_METADATA_MAP.containsKey(graphId)) {
            //test graph was never created (or was already deleted)
            return;
        }

        if(deleteGraph) {
            try {
                logger_.info("Deleting graph {}", graphId);
                new GraphDatabaseClient(graphId).deleteGraph(graphId);
                GRAPH_METADATA_MAP.remove(graphId);
            }
            catch(Exception e) {
                logger_.info("Could not delete graph {}", graphId);
            }

        }
        else {
            logger_.info("Deleting data for tenant {}", tenantId);
            getUserGraph().clear();
            getSharedGraph().clear();
        }
    }

    @Override
    public boolean isGraphScanAllowed() {
        /**
         * IBM Graph is configured to disallow queries that cannot use some index.
         */
        return false;
    }

    private static String getTenantIdFromRequestContext() {
        //TODO: Uncomment/fix when MT support is added to Atlas
        //return RequestContext.get().getTenantId();
        return null;
    }

    private static void setTenantIdInRequestContext(String tenantId) {
        //TODO: Uncomment/fix when MT support is Added to Atlas
        //RequestContext.get().setTenantId(tenantId);

    }

    @Override
    public GremlinVersion getSupportedGremlinVersion() {

        return GremlinVersion.THREE;
    }

    @Override
    public boolean requiresInitialIndexedPredicate() {
        return true;
    }

    @Override
    public GroovyExpression getInitialIndexedPredicate(GroovyExpression start) {
        GremlinStep expr = GraphDatabaseConfiguration.INSTANCE.getTenantGraphStrategy().getInitialIndexedPredicateExpr();
        if(expr == null) {
            return start;
        }
        return expr.generateGroovy(start);
    }


    @Override
    public GroovyExpression addOutputTransformationPredicate(GroovyExpression expr, boolean isSelect, boolean isPath) {

        return new TransformQueryResultExpression(expr, isSelect, isPath);
    }

    @Override
    public GroovyExpression generatePersisentToLogicalConversionExpression(GroovyExpression valueExpr, IDataType<?> type) {

        PersistentType persistentType = PersistentType.valueOf(type);
        return persistentType.generateConversionExpression(valueExpr);
    }

    @Override
    public boolean isPropertyValueConversionNeeded(IDataType<?> type) {
        PersistentType persistentType = PersistentType.valueOf(type);
        return persistentType.isPropertyValueConversionNeeded();
    }

}
