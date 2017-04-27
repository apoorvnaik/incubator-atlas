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

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.atlas.GraphInitializationException;
import org.apache.atlas.groovy.FunctionCallExpression;
import org.apache.atlas.groovy.IdentifierExpression;
import org.apache.atlas.groovy.LiteralExpression;
import org.apache.atlas.ibmgraph.api.json.Index;
import org.apache.atlas.ibmgraph.api.json.JsonEdge;
import org.apache.atlas.ibmgraph.api.json.JsonGremlinQuery;
import org.apache.atlas.ibmgraph.api.json.JsonResponse;
import org.apache.atlas.ibmgraph.api.json.Schema;
import org.apache.atlas.ibmgraph.exception.BadRequestException;
import org.apache.atlas.ibmgraph.exception.GraphDatabaseException;
import org.apache.atlas.ibmgraph.exception.HttpException;
import org.apache.atlas.ibmgraph.exception.NotFoundException;
import org.apache.atlas.ibmgraph.gremlin.GremlinQuery;
import org.apache.atlas.ibmgraph.gremlin.QueryGenerationResult;
import org.apache.atlas.ibmgraph.gremlin.expr.GetElementExpression;
import org.apache.atlas.ibmgraph.gremlin.expr.GetElementExpression.ElementType;
import org.apache.atlas.ibmgraph.http.HttpRequestHandler;
import org.apache.atlas.ibmgraph.http.IHttpRetryStrategy;
import org.apache.atlas.ibmgraph.http.RequestType;
import org.apache.atlas.ibmgraph.util.Endpoint;
import org.apache.atlas.ibmgraph.util.GraphDBUtil;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

/**
 * Client for calling into IBM Graph via its REST api.
 */
public class GraphDatabaseClient implements IGraphDatabaseClient {

    private static final Logger LOG = LoggerFactory.getLogger(GraphDatabaseClient.class.getName());

    //only use pretty printing when debug logging is enabled
    public static final Gson GSON = LOG.isDebugEnabled()
            ? new GsonBuilder().serializeNulls().setPrettyPrinting().disableHtmlEscaping().create()
            : new GsonBuilder().serializeNulls().
            disableHtmlEscaping().create();

    //don't serialize nulls in gremlin bindings, to minimize the binding size
    public static final Gson GSON_WITH_SKIP_NULLS = LOG.isDebugEnabled()
            ? new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create()
            : new GsonBuilder().disableHtmlEscaping().create();

    private HttpRequestHandler handler_ = new HttpRequestHandler();

    private String graphId;

    public GraphDatabaseClient(String graphId) {
        this.graphId = graphId;
    }

    /**
     * Constructor for testing
     *
     * @param graphId
     * @param requestHandler
     */
    public GraphDatabaseClient(String graphId, HttpRequestHandler requestHandler) {
        this.graphId = graphId;
        handler_ = requestHandler;
    }

    private JsonElement getSingleResponse(List<JsonElement> response) {
        if(response.size() > 1) {
            throw new GraphDatabaseException("Too many results came back.  Expected 1, got " + response.size());
        }
        if(response.isEmpty()) {
            return null;
        }
        return response.get(0);
    }

    //not in interface.  Used by tests.
    public JsonElement processRequest(RequestType requestType, String path, String json) {
        List<JsonElement> result = processMultiValuedRequest(requestType, path, json, null);
        return getSingleResponse(result);
    }


    private JsonElement processRequest(RequestType requestType, String path, String json, IHttpRetryStrategy strategy) {
        List<JsonElement> result = processMultiValuedRequest(requestType, path, json, strategy);
        return getSingleResponse(result);
    }

    private List<JsonElement> processMultiValuedRequest(RequestType requestType, String path,
            String json, IHttpRetryStrategy retryStrategy) {
        JsonElement jsonResponse = handler_.processRequest(requestType, path, json, retryStrategy);
        return processResponse(jsonResponse);
    }


    private JsonElement processGetRequest(Endpoint endpoint, String... pathComponents) {
        String fullQueryString = buildPath(endpoint.getUrlWithSlash(graphId), pathComponents);
        return processRequest(RequestType.GET, fullQueryString, null);
    }

    private String buildPath(String urlWithSlash, String[] pathComponents) {
        StringBuilder sb = new StringBuilder(urlWithSlash);
        if (pathComponents != null) {
            for (String q : pathComponents) {
                sb.append(q).append(Endpoint.getUrlSeparator());
            }
        }
        return sb.toString();
    }

    /**
     * @param edgeId
     * @return
     */
    @Override
    public JsonEdge getEdge(String edgeId) {
        try {
            JsonElement jsonResponse = processRequest(RequestType.GET, Endpoint.EDGES.getUrlWithSlash(graphId)+edgeId, null);
            Gson gson = GSON;
            return gson.fromJson(jsonResponse, JsonEdge.class);
        }
        catch(NotFoundException e) {
            //ok
            return null;
        }
    }

    @Override
    public Schema getSchema() throws GraphDatabaseException {
        JsonElement jsonResponse = processRequest( RequestType.GET, Endpoint.SCHEMA.getUrl(graphId), null);
        Schema schema = GSON.fromJson(jsonResponse, Schema.class);
        return schema;
    }

    @Override
    public Schema updateSchema(Schema schema, IHttpRetryStrategy strategy) throws GraphDatabaseException {

        String json = GSON.toJson(schema);
        JsonElement jsonResponse = processRequest( RequestType.POST, Endpoint.SCHEMA.getUrl(graphId), json, strategy);
        Schema result = GSON.fromJson(jsonResponse, Schema.class);
        return result;
    }

    private List<JsonElement> processResponse(JsonElement jsonResponse) {

        JsonResponse response = GSON.fromJson(jsonResponse, JsonResponse.class);
        List<JsonElement> data  = response.getResult().getData();
        if(LOG.isDebugEnabled()) {
            LOG.debug("Response data: " + data);
        }
        return data;
    }

    /**
     *
     * @param vertexId
     * @param dir
     * @param label , ignored if null
     * @return
     * @throws GraphDatabaseException
     */
    @Override
    public Collection<JsonEdge> getIncidentEdges(String vertexId, AtlasEdgeDirection dir, String label) throws GraphDatabaseException {

        IdentifierExpression graph = new IdentifierExpression("graph");
        IdentifierExpression gremlinDir = GraphDBUtil.getGremlinDirection(dir);
        GetElementExpression vertex = new GetElementExpression(graph, ElementType.VERTEX, vertexId);

        FunctionCallExpression FunctionCallExpression = new FunctionCallExpression(vertex, "edges", gremlinDir);

        if(label != null) {
            FunctionCallExpression.addArgument(new LiteralExpression(label));
        }

        GremlinQuery query = new GremlinQuery(FunctionCallExpression);
        QueryGenerationResult queryResult = query.generateGremlin();
        return executeGremlin(queryResult.getQuery(), queryResult.getParameters(), JsonEdge.class);
    }

    @Override
    public <T> List<T> executeGremlin(String query, Map<String, Object> bindings, Class<T> clazz) {
        return executeGremlin(query, bindings, clazz, GSON);
    }

    @Override
    public <T> List<T> executeGremlin(String query, Class<T> clazz) throws GraphDatabaseException {
        return executeGremlin(query, null, clazz);
    }

    @Override
    public <T> List<T> executeGremlin(String query, Map<String, Object> bindings, Class<T> clazz, Gson serializer)
            throws GraphDatabaseException {
        Type type = clazz != null ? TypeToken.get(clazz).getType() : null;
        return executeGremlin(query, bindings, type, serializer);
    }

    private <T> List<T> executeGremlin(String query, Map<String, Object> bindings, final Type type, Gson serializer)
            throws GraphDatabaseException {
        JsonGremlinQuery gremlinQuery = new JsonGremlinQuery();
        gremlinQuery.setGremlin(query);
        gremlinQuery.setBindings(bindings);

        String json = serializer.toJson(gremlinQuery);


        List<JsonElement> responseJson = processMultiValuedRequest(RequestType.POST, Endpoint.GREMLIN.getUrl(graphId), json, null);
        Function<JsonElement, T> deserializeFunction = new Function<JsonElement,T>() {
            @Override
            public T apply(JsonElement input) {
                return GSON.fromJson(input, type);
            }
        };
        return Lists.transform(responseJson, deserializeFunction);
    }


    @Override
    public Index getIndex(String indexName) {
        JsonElement jsonResponse = processGetRequest(Endpoint.INDEX, indexName);
        return GSON.fromJson(jsonResponse, Index.class);
    }

    @Override
    public void deleteIndex(String indexName) {
        processRequest(RequestType.DELETE, Endpoint.INDEX.getUrlWithSlash(graphId) + indexName, null);
    }

    @Override
    public boolean activateIndex(String name, Collection<String> props) {
        StringBuilder gremlin = new StringBuilder();
        gremlin.append("import com.ibm.titan.graphdb.database.management.IBMGraphManagement;");

        gremlin.append("import static com.thinkaurelius.titan.core.schema.SchemaStatus.REGISTERED;");
        gremlin.append(String.format("def index_name = '%s';",name));
        gremlin.append("def management = new IBMGraphManagement();");
        gremlin.append("def propsToCheck = [");
        Iterator<String> it = props.iterator();
        while(it.hasNext()) {
            gremlin.append("'");
            gremlin.append(it.next());
            gremlin.append("'");
            if(it.hasNext()) {
                gremlin.append(",");
            }
        }
        gremlin.append("] as Set<String>;");
        gremlin.append("def idx = management.getIndexStatus(graph, index_name);");
        gremlin.append("management.rollback();");
        gremlin.append("management = new IBMGraphManagement();");
        gremlin.append("def r = false;");
        gremlin.append("idx.each{ k, v -> propsToCheck.remove(k); if (((Object) v).equals(REGISTERED)) {r = true;}};");
        gremlin.append("if(! propsToCheck.isEmpty()) { r = true};");
        gremlin.append("if (r) { management.reindex(graph, index_name); };");
        gremlin.append("management.rollback();");
        gremlin.append("[idx,r];");

        try {
            List<JsonElement> result = executeGremlin(gremlin.toString(), JsonElement.class);
            return result.get(1).getAsBoolean();
        }
        catch(HttpException e) {
            //ok, this happens if the index is already up to date.  We might want to edit the gremlin script to better handle
            //this condition...
            LOG.warn("Could not run index activation script", e);
        }
        return false;
    }

    /**
     * Creates graph with given id if it does not exist yet
     * returns true if graph was created in ibm graph db.
     * false if graph already exist.
     * @param id
     * @param url
     */
    @Override
    public boolean createGraph() throws GraphInitializationException {

        if (graphExists()) {
            return false;
        }

        // We get here if graph either does not exist or is in the process
        // of being deleted.
        try {
            handler_.processRequest(RequestType.POST, Endpoint.GRAPHS.getUrlWithSlash(graphId) + graphId, null);
            return true;
        } catch (HttpException e) {
            throw new GraphInitializationException("Cannot create graph " + graphId, e);
        }

    }

    private boolean graphExists() throws GraphInitializationException {

        try {
            handler_.processRequest(RequestType.GET, Endpoint.GRAPH.getUrl(graphId), null);
            // graph exists
            return true;
        } catch (NotFoundException e) {
            return false;
        }
        catch (HttpException e) {
            throw new GraphInitializationException("Could not determine if graph " + graphId + " exists", e);
        }
    }

    @Override
    public void deleteGraph(String graphId) {

        handler_.processRequest(RequestType.DELETE, Endpoint.GRAPHS.getUrlWithSlash(graphId) + graphId, null);
    }

    @Override
    public IndexStatus getOverallStatus(String name, Collection<String> expectedProperties) {
        IndexStatus overallStatus = null;
        Map<String,IndexStatus> status = getIndexStatus(name);
        for(String property : expectedProperties) {
            IndexStatus propertyStatus = status.get(property);
            if(propertyStatus == null) {
                overallStatus = IndexStatus.NOT_PRESENT;
            }
            else if(overallStatus == null || propertyStatus.isWorseThan(overallStatus)) {
                overallStatus = propertyStatus;
            }
        }
        return overallStatus;

    }

    private Map<String,IndexStatus> getIndexStatus(String name) {
        Map<String,IndexStatus> result = new HashMap<>();
        JsonElement jsonResponse = processRequest(RequestType.GET, Endpoint.INDEX.getUrlWithSlash(graphId) + name + "/status", null);
        JsonObject object = jsonResponse.getAsJsonObject();
        //for composite indices, returns the status of each element in the index
        //for mixed indices, has one property with the status of the index
        for(Map.Entry<String,JsonElement> entry : object.entrySet()) {
            String statusString = entry.getValue().getAsString();
            IndexStatus status = IndexStatus.valueOf(statusString);
            result.put(entry.getKey(), status);
        }
        return result;
    }

    @Override
    public Collection<String> getPropertyKeysInIndex(String indexName) {
        try {
            return getIndexStatus(indexName).keySet();
        }
        catch(BadRequestException e) {
            if(e.getResponse().toString().contains("does not exist")) {
                return Collections.emptyNavigableSet();
            }
            throw e;
        }

    }

}
