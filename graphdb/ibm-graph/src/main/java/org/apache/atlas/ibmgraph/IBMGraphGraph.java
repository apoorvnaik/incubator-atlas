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
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.groovy.FunctionCallExpression;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.GroovyGenerationContext;
import org.apache.atlas.groovy.IdentifierExpression;
import org.apache.atlas.groovy.ListExpression;
import org.apache.atlas.groovy.LiteralExpression;
import org.apache.atlas.groovy.VariableAssignmentExpression;
import org.apache.atlas.ibmgraph.api.GraphDatabaseClient;
import org.apache.atlas.ibmgraph.api.GraphDatabaseConfiguration;
import org.apache.atlas.ibmgraph.api.IGraphDatabaseClient;
import org.apache.atlas.ibmgraph.api.action.ClearGraphAction;
import org.apache.atlas.ibmgraph.api.action.IGraphAction;
import org.apache.atlas.ibmgraph.api.json.Index;
import org.apache.atlas.ibmgraph.api.json.JsonEdge;
import org.apache.atlas.ibmgraph.api.json.JsonIndexQueryResult;
import org.apache.atlas.ibmgraph.api.json.JsonVertex;
import org.apache.atlas.ibmgraph.api.json.JsonVertexData;
import org.apache.atlas.ibmgraph.api.json.update.UpdateScriptBinding;
import org.apache.atlas.ibmgraph.exception.GraphDatabaseException;
import org.apache.atlas.ibmgraph.exception.HttpException;
import org.apache.atlas.ibmgraph.gremlin.ActionTranslationContext;
import org.apache.atlas.ibmgraph.gremlin.GremlinQuery;
import org.apache.atlas.ibmgraph.gremlin.QueryGenerationResult;
import org.apache.atlas.ibmgraph.gremlin.stmt.PreGeneratedStatement;
import org.apache.atlas.ibmgraph.tx.IBMGraphTransaction;
import org.apache.atlas.ibmgraph.util.FileUtils;
import org.apache.atlas.ibmgraph.util.GraphDBUtil;
import org.apache.atlas.ibmgraph.util.PropertyIndex;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.utils.LruCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptEngine;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



/**
 * Represents a graph in IBM Graph.  There is one IBMGraphGraph instance per tenant.  This
 * instance is shared by all threads associated with the tenant.
 */
public class IBMGraphGraph implements AtlasGraph<IBMGraphVertex, IBMGraphEdge> {

    private static final String LOGGING_OFF_TOKEN = "</IF_LOGGING>";
    private static final String LOGGING_ON_TOKEN = "<IF_LOGGING>";
    private static final Logger logger_ = LoggerFactory.getLogger(IBMGraphGraph.class.getName());
    private static final Logger gremlinUpdateLogger_ = LoggerFactory.getLogger("GraphUpdateScriptLogger");

    private final IBMGraphDatabase parent_;
    private final String tenantId_;

    //This is thread local because there could be concurrent requests that use the same tenant.  Each request is handled
    //by a separate thread in Jetty.  All of those simultaneous requests use the same instance of IBMGraphGraph.  Having
    //a thread local variable makes it so that each thread has its own transaction.

    //The variable is not static because within a thread we need to be able to have a transaction for both the global
    //graph and the tenant graph.  There are separate instances of IBMGraphGraph graph for the global and tenant graph, but
    //with a static variable the transaction would be shared.  Having the variable non-static also removes the possibility of
    //having using a transaction that is associated with a different graph instance.
    private final ThreadLocal<IBMGraphTransaction> transactionWrapper_ = new ThreadLocal<>();

    // Cache of vertices/edges that have been created and explicitly retrieved.
    // Note: creation of cache entries needs to be governed by a shared lock (see OMS-764 as for what can happen otherwise).

    private final ReentrantReadWriteLock cacheLock_ = new ReentrantReadWriteLock();

    //synchronizes the rest api calls associated with this logical graph.  We take a read
    //lock when performing queries, and a write lock when performing updates.  This follows
    //the many readers, one writer paradigm.  This lock does not cover index
    //operations.
    private final ReentrantReadWriteLock graphLock_ = new ReentrantReadWriteLock();

    private final LruCache<String, IBMGraphVertex> knownVertices_ = new LruCache<>(
            GraphDatabaseConfiguration.getElementCacheCapacity(),
            GraphDatabaseConfiguration.getElementCacheEvictionWarningThrottle());

    private final LruCache<String, IBMGraphEdge> knownEdges_ = new LruCache<>(
            GraphDatabaseConfiguration.getElementCacheCapacity(),
            GraphDatabaseConfiguration.getElementCacheEvictionWarningThrottle());

    private final IGraphDatabaseClient client_;

    private final static String GRAPH_UPDATE_SCRIPT = loadGraphUpdateScript();



    private final IBMGraphMetadata metadata;

    public IBMGraphGraph(IBMGraphDatabase parent, IBMGraphMetadata metadata, String tenantId)  {
        parent_ = parent;
        this.metadata = metadata;
        client_ = addReadLockWrapper(new GraphDatabaseClient(getGraphId()));
        tenantId_ = tenantId;

        this.beginTransaction();
    }


    @Override
    public void clear() {
        logger_.warn("Clearing graph {}", this);
        //commit() first to force pending changes to be applied before clearing.  This
        //is mostly for testing purposes, so we can be sure that all the gremlin we
        //generate is valid and can execute without throwing an exception.
        commit();
        apply(new ClearGraphAction(), JsonElement.class);
    }

    /**
     * Creates an instance of AtlasGraphManagement.
     */
    @Override
    public AtlasGraphManagement getManagementSystem() {

        return new IBMGraphManagement(metadata, client_, tenantId_);
    }

    @Override
    public void exportToGson(OutputStream os) throws IOException {
        //TBD
    }

    @Override
    public ScriptEngine getGremlinScriptEngine() throws AtlasBaseException {
        // TODO: Complete implementation here
        return null;
    }

    @Override
    public void releaseGremlinScriptEngine(ScriptEngine scriptEngine) {
        // TODO: Complete implementation here
    }

    @Override
    public AtlasEdge<IBMGraphVertex, IBMGraphEdge> addEdge(AtlasVertex<IBMGraphVertex, IBMGraphEdge> outVertex,
            AtlasVertex<IBMGraphVertex, IBMGraphEdge> inVertex, String label) {

        if(inVertex == null) {
            throw new IllegalStateException("The incoming edge cannot be null.");
        }

        if(outVertex == null) {
            throw new IllegalStateException("The outgoing edge cannot be null.");
        }


        if(inVertex.getV().isDeleted() || inVertex.getV().isDeleted()) {
            throw new IllegalStateException("You cannot add an edge to or from a deleted vertex");
        }

        AtlasEdge<IBMGraphVertex, IBMGraphEdge> result =
                getTx().addEdge(outVertex.getV(), inVertex.getV(), label);
        result = GraphDBUtil.addEdgeProxyWrapper(result);

        return result;
    }

    @Override
    public AtlasVertex<IBMGraphVertex, IBMGraphEdge> addVertex() {
        AtlasVertex<IBMGraphVertex, IBMGraphEdge> result = getTx().addVertex();
        result = GraphDBUtil.addVertexProxyWrapper(result);
        GraphDatabaseConfiguration.INSTANCE.getTenantGraphStrategy().updateNewVertexIfNeeded(result.getV(), getGraphId(), getTenantId());
        return result;

    }

    /**
     * This is called to indicate that the given element has been
     * assigned an id.
     *
     * @param element
     */
    public void onIdAssigned(IBMGraphElement<?> element) {
        logger_.debug("onIdAssigned called for {}", element);
        try {
            cacheLock_.writeLock().lock();
            if (element instanceof IBMGraphVertex) {
                knownVertices_.put(element.getIdString(), (IBMGraphVertex)element);
            } else {
                knownEdges_.put(element.getIdString(), (IBMGraphEdge)element);
            }
        }
        finally {
            cacheLock_.writeLock().unlock();
        }
    }

    @Override
    public void removeEdge(AtlasEdge<IBMGraphVertex, IBMGraphEdge> edge) {
        logger_.debug("Removing edge {}", edge);
        getTx().removeEdge(edge);
    }

    @Override
    public void removeVertex(AtlasVertex<IBMGraphVertex, IBMGraphEdge> vertex) {
        logger_.debug("Removing vertex {}", vertex);
        getTx().removeVertex(vertex);
    }

    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasGraph#getEdge(java.lang.String)
     */
    @Override
    public AtlasEdge<IBMGraphVertex, IBMGraphEdge> getEdge(String edgeId) {

        IBMGraphEdge result = lookupEdgeWithReadLock(edgeId);
        if (result == null) {
           try {
                cacheLock_.writeLock().lock();
                result = knownEdges_.get(edgeId);
                if (result == null) {
                    result = new IBMGraphEdge(this, edgeId);
                    knownEdges_.put(edgeId, result);
                }
            }
            finally {
                cacheLock_.writeLock().unlock();
            }
        }
        if (result != null && result.isDeleted()) {
            result = null;
        }
        return result;
    }

    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasGraph#getEdges()
     */
    @Override
    public Iterable<AtlasEdge<IBMGraphVertex, IBMGraphEdge>> getEdges() {
        //TODO
        return new ArrayList<>();
    }


    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasGraph#getVertices()
     */
    @Override
    public Iterable<AtlasVertex<IBMGraphVertex, IBMGraphEdge>> getVertices() {
        return new ArrayList<>();
    }


    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasGraph#getVertex(java.lang.String)
     */
    @Override
    public AtlasVertex<IBMGraphVertex, IBMGraphEdge> getVertex(String vertexId) {

        IBMGraphVertex result = getVertexWithoutDeleteCheck(vertexId);
        if (result != null && result.isDeleted()) {
            result = null;
        }
        return result;
    }

    /**
     * Gets or creates a vertex with the given id.  Guaranteed to return a
     * non-null result.
     *
     * @param vertexId
     * @return
     */
    public IBMGraphVertex getVertexWithoutDeleteCheck(String vertexId) {

        IBMGraphVertex result = lookupVertexWithReadLock(vertexId);
        if (result == null) {
            try {
                cacheLock_.writeLock().lock();
                result = knownVertices_.get(vertexId);
                if (result == null) {
                    result = new IBMGraphVertex(this, vertexId);
                    knownVertices_.put(vertexId, result);
                }
            }
            finally {
                cacheLock_.writeLock().unlock();
            }
        }
        return result;
    }

    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasGraph#getEdgeIndexKeys()
     */
    @Override
    public Set<String> getEdgeIndexKeys() {
        // TODO Auto-generated method stub
        return null;
    }


    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasGraph#getVertexIndexKeys()
     */
    @Override
    public Set<String> getVertexIndexKeys() {
        // TODO Auto-generated method stub
        return null;
    }


    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasGraph#getVertices(java.lang.String, java.lang.Object)
     */
    @Override
    public Iterable<AtlasVertex<IBMGraphVertex, IBMGraphEdge>> getVertices(String key, Object value) {

        return query().has(key, value).vertices();
    }


    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasGraph#query()
     */
    @Override
    public AtlasGraphQuery<IBMGraphVertex, IBMGraphEdge> query() {

        return new IBMGraphGraphQuery(this);
    }


    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasGraph#indexQuery(java.lang.String, java.lang.String)
     */
    @Override
    public AtlasIndexQuery<IBMGraphVertex, IBMGraphEdge> indexQuery(String indexName, String queryString) {
        applyPendingChanges();
        return new IBMGraphIndexQuery(this, indexName, queryString);
    }

    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasGraph#commit()
     */
    @Override
    public void commit() {
        try {
            logger_.info(getLogPrefix() + " commiting transaction: " + getTx().getId());
            getTx().commit();
        }
        catch(Throwable t) {
            logger_.error(getLogPrefix() +"Could not commit transaction", t);
            try {
                rollback();
            }
            catch(Throwable t2) {
                logger_.error(getLogPrefix() +"Could not roll back transaction either", t2);
            }
            throw t;
        }
        finally {
            transactionWrapper_.set(null);
        }
    }


    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasGraph#rollback()
     */
    @Override
    public void rollback() {

        try {
            cacheLock_.writeLock().lock();
            getTx().rollback();
        }
        finally {
            cacheLock_.writeLock().unlock();
            transactionWrapper_.set(null);
        }
    }


    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasGraph#shutdown()
     */
    @Override
    public void shutdown() {
        commit();
    }

    public void applyUpdates(UpdateScriptBinding binding) {
        IGraphDatabaseClient client = getClient();
        try {
            graphLock_.writeLock().lock();
            List<JsonElement> result = client.executeGremlin(GRAPH_UPDATE_SCRIPT, binding.getGremlinParameters(),
                    JsonElement.class, GraphDatabaseClient.GSON_WITH_SKIP_NULLS);
            boolean success = result.get(0).getAsBoolean();
            int startIdx = 1;
            if(updateScriptLoggingEnabled()) {
                //logging enabled.  Last argument is log.
                String log = result.get(1).getAsString();
                gremlinUpdateLogger_.debug(log);
                startIdx++;
            }
            if(success) {
                JsonArray createdVertices = result.get(startIdx).getAsJsonArray();
                JsonArray createdEdges = result.get(startIdx+1).getAsJsonArray();
                int i = 0;
                for (IBMGraphVertex vertex : binding.getIncludedNewVertices()) {
                    String id = createdVertices.get(i++).getAsString();
                    vertex.assignId(id);
                }
                int j = 0;
                for (IBMGraphEdge edge : binding.getIncludedNewEdges()) {
                    String id = createdEdges.get(j++).getAsString();
                    edge.assignId(id);
                }
            }
            else {
                String error = result.get(startIdx).getAsString();
                throw new GraphDatabaseException("Could not apply " + binding + ": " + error);

            }
        }
        catch (HttpException e) {
            throw new GraphDatabaseException("Could not apply updates: " + binding.toString(), e);
        }
        finally {
            graphLock_.writeLock().unlock();
        }

    }

    private static boolean updateScriptLoggingEnabled() {
        return gremlinUpdateLogger_.isDebugEnabled();
    }

    @Override
    public Object executeGremlinScript(String query, boolean isPath) throws AtlasBaseException {
        return executeGremlinScript(query, null, isPath);
    }
    @Override
    public Object executeGremlinScript(String query, Map<String,Object> bindings, boolean isPath) throws AtlasBaseException {

        Object rawResult = executeGremlinScript(query, bindings);
        List<Object> toConvert = null;

        //For path queries (queries with a path() call in them), we return the path
        //that was generated, as a list.  The first step for those queries is to
        //extract the path from the json result.
        if(isPath) {
            toConvert = convertPathQueryResultToList(rawResult);
        }
        else {
            toConvert = (List)rawResult;
        }

        //Recursively convert the raw list items we got back from json classes into normal
        //java types and AtlasVertex/AtlasEdge values that Atlas expects.
        List<Object> result = new ArrayList<>(toConvert.size());
        for(Object o : toConvert) {
            result.add(convertJsonElement(o));
        }
        return result;
    }

    private Object executeGremlinScript(String gremlinQuery, Map<String, Object> bindings) throws AtlasBaseException {

        //to be consistent with Titan, don't push the changes when executing a query.  The query
        //results may be stale, but that is expected.
        if(getTx().hasPendingChanges()) {
            logger_.warn("{}: Executing gremlin query while there are uncommitted changes: {}", getLogPrefix(), gremlinQuery);
            logger_.debug("Location: ", new Exception());
            logger_.debug("Transaction started at: ", getTx().getCreationStack());
        }

        GroovyExpression stmt = new PreGeneratedStatement(gremlinQuery);
        GroovyExpression updated = GraphDBUtil.addDefaultInitialStatements(tenantId_, stmt);
        StringBuilder query = new StringBuilder();
        GroovyGenerationContext ctx = new GroovyGenerationContext();
        ctx.setParametersAllowed(false);
        updated.generateGroovy(ctx);
        query.append(ctx.getQuery());

        long start = System.currentTimeMillis();
        Object result = getClient().executeGremlin(query.toString(), bindings, JsonElement.class);
        if(logger_.isDebugEnabled()) {
            long elapsed = System.currentTimeMillis() - start;
            logger_.debug(gremlinQuery + " completed in " + elapsed + " ms");
        }
        return result;
    }


    private Object convertJsonElement(Object rawValue) {
        if(rawValue instanceof JsonElement) {
            JsonElement elem = (JsonElement)rawValue;
            if(elem.isJsonPrimitive()) {
                return convertJsonPrimitive(elem.getAsJsonPrimitive());
            }

            if(elem.isJsonObject()) {
                return convertJsonObject(elem.getAsJsonObject());
            }

            if(elem.isJsonArray()) {
                return convertJsonArray(elem.getAsJsonArray());
            }
            if(elem.isJsonNull()) {
                return null;
            }
        }

        throw new IllegalStateException("Unrecognized value: " + rawValue + " of class " + rawValue.getClass() + ".  Expected a JsonObject, JsonPrimitive, or JsonArray.");
    }

    private Object convertJsonArray(JsonArray array) {
        List<Object> result = new ArrayList<Object>();
        for(int i = 0; i < array.size(); i++) {
            result.add(convertJsonElement(array.get(i)));
        }
        return result;
    }

    private Object convertJsonObject(JsonObject obj) {
        JsonElement typeElem = obj.get("type");
        Gson gson = GraphDBUtil.getGson();
        //In cases where the gremlin query has a select call with multiple labels in it where one of those labels is 'type' such
        //as "select('type','name')", "type" will be a key in the JSON object that comes back in the query result (when there is only one label,
        //a JsonObject is not created, we just get back the value that was selected).  In that case, the value of the "type" key in the JsonObject is
        //the stuff that was selected for that key via the by() clause in the query.  We need to be careful not to try to convert objects
        //like this into edges or vertices even though they happen to have a key named "type".  Conveniently, Atlas always
        //makes the value we are selecting be an array when generating the Gremlin for DSL queries.  As a result, it is sufficient to
        //check if the type value is a primitive to see whether we are in a case like that.  If we are, we skip the logic
        //to try to convert the object to an AtlasVertex or AtlasEdge and directly convert the object to a Map.  There still is some
        //exposure here, if allow users to directly call the gremlin endpoint.  They could theoretically use a gremlin query
        //with a select('type',...) that happens to select the value "vertex","edge",or "vertexdata".  In that case, we would likely
        //end up creating an invalid JsonVertex/JsonEdge.  A better solution would be to change the query api so that we know
        //whether a select query is being executed (and whether there is more than one label in the select clause).  In that
        //case, we know that a map will come back and we can handle it as a map.  At this point, though, we are not allowing
        //arbitrary gremlin queries to be used, so this is not an issue.  One other possibility is that when we execute queries like
        //this, the json we get back from IBM Graph includes the labels from the select in the result.  We could use those to determine
        //whether or not we're getting a map back.
        if (typeElem != null && typeElem.isJsonPrimitive()) {
            String type = typeElem.getAsString();
            if (type.equals("vertex")) {
                JsonVertex vertex = gson.fromJson(obj, JsonVertex.class);
                JsonVertexData vertexData = new JsonVertexData(vertex);
                return wrapVertex(vertexData, true);
            } else if (type.equals("edge")) {
                JsonEdge edge = gson.fromJson(obj, JsonEdge.class);
                return wrapEdge(edge, true);
            } else if (type.equals("vertexdata")) {
                JsonVertexData vertexData = gson.fromJson(obj,
                        JsonVertexData.class);
                return wrapVertex(vertexData, true);
            }
            //not a known type.  Fall through and convert
            //the object to a map.
        }
        Map<String, Object> resultMap = new HashMap<String, Object>();
        for (Entry<String, JsonElement> jsonEntry: obj.entrySet()) {
            resultMap.put(jsonEntry.getKey(), convertJsonElement(jsonEntry.getValue()));
        }
       return resultMap;
    }



    private Object convertJsonPrimitive(JsonPrimitive prim) {

        if(prim.isNumber()) {
            return prim.getAsNumber();
        }
        if(prim.isBoolean()) {
            return prim.getAsBoolean();
        }

        return prim.getAsString();
    }

    private List<Object> convertPathQueryResultToList(Object rawValue) {

        List array = null;
        if(rawValue instanceof List) {
            array = (List)rawValue;
        }
        else {
            Map obj = (Map)rawValue;
            array =(List)obj.get("objects");
        }
        return array;
    }

    public PropertyIndex getPropertyChangeIndex() {
        return getTx().getPropertyChangeIndex();
    }

    public AtlasEdge<IBMGraphVertex, IBMGraphEdge> wrapEdge(JsonEdge edge, boolean addProxy) {

        if (edge == null) {
            return null;
        }
        String id = edge.getId();
        IBMGraphEdge result = lookupEdgeWithReadLock(id);
        if (result == null) {
            try {
                cacheLock_.writeLock().lock();
                result = knownEdges_.get(id);
                if (result == null) {
                    result = new IBMGraphEdge(this, edge);
                    knownEdges_.put(id, result);
                }
            }
            finally {
                cacheLock_.writeLock().unlock();
            }
        }
        if (result == null || result.isDeleted()) {
            return null;
        }
        if (addProxy) {
            return GraphDBUtil.addEdgeProxyWrapper(result);
        }
        return result;
    }

    public AtlasVertex<IBMGraphVertex, IBMGraphEdge> wrapVertex(JsonVertex vertex, boolean addProxy) {
        return wrapVertex(new JsonVertexData(vertex), addProxy);
    }
    public IBMGraphVertex wrapVertex(JsonVertexData vertex) {
        return wrapVertex(vertex, false).getV();
    }

    private IBMGraphEdge lookupEdgeWithReadLock(String id) {
        try {
            cacheLock_.readLock().lock();
            IBMGraphEdge result = knownEdges_.get(id);
            return result;
        }
        finally {
            cacheLock_.readLock().unlock();
        }
    }


    private IBMGraphVertex lookupVertexWithReadLock(String id) {
        try {
            cacheLock_.readLock().lock();
            IBMGraphVertex result = knownVertices_.get(id);
            return result;
        }
        finally {
           cacheLock_.readLock().unlock();
        }
    }

    public AtlasVertex<IBMGraphVertex, IBMGraphEdge> wrapVertex(JsonVertexData vertex, boolean addProxy) {

        if (vertex == null) {
            return null;
        }
        String id = vertex.getVertex().getId();

        IBMGraphVertex result = lookupVertexWithReadLock(id);
        if (result == null) {
            cacheLock_.writeLock().lock();
            try {
                result = knownVertices_.get(id);
                if (result == null) {
                    result = new IBMGraphVertex(this, vertex.getVertex());
                    // Make sure that we set the outgoing edges *after* updating
                    // known vertices cache, since setting the
                    // outgoing edges will create wrappers for the json edges
                    // and ultimately need to get this new vertex from the cache.
                    // If it is not there at that point, a phantom vertex will be created.
                    knownVertices_.put(id, result);
                    result.setOutgoingEdges(vertex.getOutgoingEdges());
                }
            }
            finally {
                cacheLock_.writeLock().unlock();
            }
        }

        if (result == null || result.isDeleted()) {
            return null;
        }
        if (addProxy) {
            return GraphDBUtil.addVertexProxyWrapper(result);
        }
        return result;
    }

    public Collection<AtlasVertex<IBMGraphVertex, IBMGraphEdge>> wrapVertices(
            Collection<JsonVertexData> jsonResult, final boolean addProxy) {

        Collection<AtlasVertex<IBMGraphVertex, IBMGraphEdge>> temp =  Collections2.transform(jsonResult, getVertexWrapperFunction(addProxy));

        //filter out deleted vertices
        return Collections2.filter(temp, new Predicate<AtlasVertex<IBMGraphVertex, IBMGraphEdge>>() {

            @Override
            public boolean apply(AtlasVertex<IBMGraphVertex, IBMGraphEdge> input) {
                return input != null ;
            }
        });
    }

    public Collection<AtlasEdge<IBMGraphVertex,IBMGraphEdge>> wrapEdges(Collection<JsonEdge> jsonResult, final boolean addProxy) {

        if(jsonResult == null) {
            return null;
        }
        Function<JsonEdge, AtlasEdge<IBMGraphVertex, IBMGraphEdge>> edgeWrapperFunction = getEdgeWrapperFunction(addProxy);
        Collection<AtlasEdge<IBMGraphVertex,IBMGraphEdge>> result =  new HashSet<>(jsonResult.size());
        for(JsonEdge jsonEdge : jsonResult) {
            AtlasEdge<IBMGraphVertex, IBMGraphEdge> edge = edgeWrapperFunction.apply(jsonEdge);
            if(edge != null) {
                result.add(edge);
            }
        }
        return result;
    }

    public IBMGraphIndexQueryResult wrapIndexQueryResult(JsonIndexQueryResult result) {
        return new IBMGraphIndexQueryResult(wrapVertex(result.getVertex(), true), result.getScore());
    }

    public IBMGraphDatabase getParent() {
        return parent_;
    }

    public void deleteIndex (String indexName) {
        getClient().deleteIndex(indexName);
    }

    public Index getIndex (String indexName) {
        return getClient().getIndex(indexName);
    }

    public void applyPendingChanges() {
        getTx().applyPendingChanges();
    }

    private JsonElement processQueryResult(GeneratedGremlinQuery expr, List<JsonElement> output) {

        JsonElement elementIds = output.get(1);
        int  i = 0;
        for(IBMGraphElement element : expr.getNewElements()) {
            element.assignId(elementIds.getAsJsonArray().get(i++).getAsString());
        }

        return output.get(0);
    }



    public static class GeneratedGremlinQuery {
        //query result format:
        //[returnValue, [element1Id, element2Id, etc]]
        private QueryGenerationResult query_;
        private List<IBMGraphElement> newElements_;

        public GeneratedGremlinQuery(QueryGenerationResult query, List<IBMGraphElement> newElements_) {
            super();
            query_ = query;
            this.newElements_ = newElements_;
        }

        public String getQuery() {
            return query_.getQuery();
        }

        public List<IBMGraphElement> getNewElements() {
            return newElements_;
        }

        public Map<String,Object> getQueryParmemeters() {
            return query_.getParameters();
        }
    }
    public GeneratedGremlinQuery generateGremlin(List<IGraphAction> actions, boolean forceReturnNull) {
        return generateGremlin(actions, forceReturnNull, true);
    }

    public GeneratedGremlinQuery generateGremlin(List<IGraphAction> actions, boolean forceReturnNull,
                                                 boolean useQueryParameters) {

        ActionTranslationContext context = new ActionTranslationContext(this);

        List<GroovyExpression> stmts = generateGremlinStmts(actions, context);
        if(stmts.isEmpty()) {
            GroovyExpression newLast = new VariableAssignmentExpression("result", LiteralExpression.NULL);
            stmts.add(newLast);
        }
        else {
            if(! forceReturnNull) {
                //convert last statement into a variable assignment, include the result
                //in the return value
                GroovyExpression last = stmts.remove(stmts.size() - 1);
                stmts.add(new VariableAssignmentExpression("result", (GroovyExpression)last));
            }
            else {
                GroovyExpression newLast = new VariableAssignmentExpression("result", LiteralExpression.NULL);
                stmts.add(newLast);
            }
        }

        stmts.addAll(0, context.getVariableDefinitions());
        GroovyExpression newElementIdList = getGeneratedElementIdList(context);

        stmts.add(new ListExpression(new IdentifierExpression("result"), newElementIdList));

        GremlinQuery query = new GremlinQuery(stmts);
        QueryGenerationResult generatedQuery = query.generateGroovy(useQueryParameters);

        return new GeneratedGremlinQuery(generatedQuery, context.getNewElements());
    }

    private  List<GroovyExpression> generateGremlinStmts(List<IGraphAction> actions, ActionTranslationContext context) {
        List<GroovyExpression> stmts = new ArrayList<GroovyExpression>();
        for(IGraphAction action : actions) {
            List<GroovyExpression> actionStmts = action.generateGremlinQuery(context);
            if(actionStmts != null) {
                stmts.addAll(actionStmts);
            }
        }
        return stmts;
    }

    private GroovyExpression getGeneratedElementIdList(ActionTranslationContext context) {
        List<IBMGraphElement> newElements = context.getNewElements();
        List<GroovyExpression> idExprs = new ArrayList<>();
        //retrieve the ids of the newly created elements, update them
        for(IBMGraphElement newElement : newElements) {
            idExprs.add(new FunctionCallExpression(context.getElementReferenceExpression(newElement), "id"));
        }
        GroovyExpression expr = new ListExpression(idExprs);
        return expr;
    }

    /**
     * @param action
     */
    public <T> T apply(IGraphAction action, Class<T> type) {
        try {
            //We can use a readlock here since this is method is only used for clearing and
            //resolving proxies.  Clearing is only done by tests.  There is no need
            //to add the overhead of obtaining a write lock just for that.
            graphLock_.readLock().lock();
            GeneratedGremlinQuery gremlin = generateGremlin(Collections.singletonList(action), false);
            List<JsonElement> overallResult = getClient().executeGremlin(gremlin.getQuery(), gremlin.getQueryParmemeters(), JsonElement.class);
            JsonElement queryResult = overallResult.get(0);
            return GraphDatabaseClient.GSON.fromJson(queryResult, type);
        }
        finally {
            graphLock_.readLock().unlock();
        }
    }

    public Function<JsonIndexQueryResult, AtlasIndexQuery.Result<IBMGraphVertex, IBMGraphEdge>> getIndexQueryResultWrapperFunction() {
        Function<JsonIndexQueryResult,AtlasIndexQuery.Result<IBMGraphVertex, IBMGraphEdge>> result = new Function<JsonIndexQueryResult,AtlasIndexQuery.Result<IBMGraphVertex, IBMGraphEdge>>() {
            @Override
            public IBMGraphIndexQueryResult apply(JsonIndexQueryResult input) {
                return wrapIndexQueryResult(input);
            }
        };
        return result;
    }

    public Function<JsonVertexData, AtlasVertex<IBMGraphVertex,IBMGraphEdge>> getVertexWrapperFunction(final boolean addProxy) {
        Function<JsonVertexData, AtlasVertex<IBMGraphVertex,IBMGraphEdge>> result =
                new Function<JsonVertexData, AtlasVertex<IBMGraphVertex,IBMGraphEdge>>() {

            @Override
            public AtlasVertex<IBMGraphVertex,IBMGraphEdge> apply(JsonVertexData vertex) {
                return wrapVertex(vertex, addProxy);
            }
        };
        return result;
    }

    private Function<JsonEdge,AtlasEdge<IBMGraphVertex,IBMGraphEdge>> getEdgeWrapperFunction(final boolean addProxy) {
        Function<JsonEdge, AtlasEdge<IBMGraphVertex,IBMGraphEdge>> result = new Function<JsonEdge, AtlasEdge<IBMGraphVertex,IBMGraphEdge>>() {
            @Override
            public AtlasEdge<IBMGraphVertex,IBMGraphEdge> apply(JsonEdge edge) {
                return wrapEdge(edge, addProxy);
            }
        };
        return result;
    }

    //only for testing
    public void flushCache() {
        try {
            cacheLock_.writeLock().lock();
            knownEdges_.clear();
            knownVertices_.clear();
        }
        finally {
            cacheLock_.writeLock().unlock();
        }
        getTx().rollback();
    }

    /**
     * @return
     */
    public IGraphDatabaseClient getClient() {
        return client_;
    }

    public IBMGraphTransaction getTx() {
        IBMGraphTransaction transaction = transactionWrapper_.get();
        if(transaction == null) {
            //this should be safe, since each thread will only
            //have one graph associated with it
            transaction = beginTransaction();
        }
        if (transaction.getParent() != this) {
            String msg = "Transaction " + transaction
                    + " is associated with a different graph: " + transaction.getParent().toString() + ".  current graph is:" + toString();
            throw new IllegalStateException(msg);
        }
        return transaction;
    }

    public IBMGraphTransaction beginTransaction() {
        IBMGraphTransaction transaction = new IBMGraphTransaction(this);
        transactionWrapper_.set(transaction);

        logger_.info(getLogPrefix() + " begin transaction " + transaction.getId());
        return transaction;
    }

    /**
     * Returns graph id of graph
     * @return
     */
    public String getGraphId() {
        return metadata.getGraphName();
    }

    public String getLogPrefix() {
        return this.getGraphId() + ":" + this.getTenantId() + ": "
                + (transactionWrapper_.get() != null ? transactionWrapper_.get().getId() + ": " : "");
    }


    //for testing only
    public int getEdgeCacheSize() {
        try {
            cacheLock_.readLock().lock();
            return knownEdges_.size();
        }
        finally {
            cacheLock_.readLock().unlock();
        }
    }

    public int getVertexCacheSize() {
        try {
            cacheLock_.readLock().lock();
            return knownVertices_.size();
        }
        finally {
            cacheLock_.readLock().unlock();
        }

    }

    private static String loadGraphUpdateScript() {
        try {

            boolean enableLogging = updateScriptLoggingEnabled();

            String query = FileUtils.readStream(
                    Thread.currentThread().getContextClassLoader().getResourceAsStream("src/main/resources/graphUpdater.groovy"));

            GroovyExpression tenantIdExpr = new IdentifierExpression("tenantId");
            GroovyExpression traversalExpr = GraphDatabaseConfiguration.INSTANCE.getTenantGraphStrategy().getGraphTraversalExpression(tenantIdExpr);
            GroovyGenerationContext ctx = new GroovyGenerationContext();
            ctx.setParametersAllowed(false);
            traversalExpr.generateGroovy(ctx);
            String traversalGremlin  =ctx.getQuery();
            query = query.replaceAll(Pattern.quote("<TRAVERSAL_SOURCE_EXPRESSION>"), Matcher.quoteReplacement(traversalGremlin));
            if(enableLogging) {
                query = query.replaceAll(Pattern.quote(LOGGING_ON_TOKEN),"");
                query = query.replaceAll(Pattern.quote(LOGGING_OFF_TOKEN),"");
            }
            else {
                //use relucant quantifier .*? to ensure that we only replace the current logging on block.
                query = query.replaceAll("(?s)" + Pattern.quote(LOGGING_ON_TOKEN) + ".*?" + Pattern.quote(LOGGING_OFF_TOKEN),"");
            }

            //remove line comments
            query = query.replaceAll("(?m)\\s*//.*$", " ");

            //remove multi-line comments
            query = query.replaceAll("(?m)(?s)\\s*/\\*.*?\\*/\\s*", " ");

            //remove new lines
            query = query.replaceAll("[\r\n]+", " ");

            //collapse white-space
            query = query.replaceAll("\\s+", " ");



            return query;
        } catch (IOException e) {
            throw new RuntimeException("Could not load graph update script!", e);
        }

    }

    @Override
    public boolean isMultiProperty(String name) {
        AtlasGraphManagement mgmt = getManagementSystem();
        try {
            AtlasPropertyKey key = mgmt.getPropertyKey(name);
            if(key == null) {
                return false;
            }
            return key.getCardinality().isMany();
        }
        finally {
            mgmt.rollback();
        }
    }

    public String toString() {
        return "IBMGraphGraph@" + Integer.toHexString(hashCode()) + "[id="+getGraphId()+ ", tenant=" + getTenantId() + "]";
    }

    public Map<String,AtlasPropertyKey> getExistingPropertyKeys() {
        return metadata.getExistingPropertyKeys();
    }

    public Map<String,Index> getExistingIndices() {
        return metadata.getExistingIndices();
    }

    public String getTenantId() {
        return tenantId_;
    }
    /**
     * @return
     */
    public ReentrantReadWriteLock getGraphLock() {
        return graphLock_;
    }

    private IGraphDatabaseClient addReadLockWrapper(GraphDatabaseClient client) {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        GraphReadLockInvocationHandler handler = new GraphReadLockInvocationHandler(client, this);
        return (IGraphDatabaseClient)Proxy.newProxyInstance(classloader, new Class[]{IGraphDatabaseClient.class}, handler);
    }
}
