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
package org.apache.atlas.repository.graphdb;

import org.apache.atlas.exception.AtlasBaseException;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Set;

/**
 * Represents a graph.
 *
 * @param <V> vertex implementation class
 * @param <E> edge implementation class
 */
public interface AtlasGraph<V, E> {

    /**
     * Adds an edge to the graph.
     *
     * @param outVertex
     * @param inVertex
     * @param label
     * @return
     */
    AtlasEdge<V, E> addEdge(AtlasVertex<V, E> outVertex, AtlasVertex<V, E> inVertex, String label);

    /**
     * Adds a vertex to the graph.
     *
     * @return
     */
    AtlasVertex<V, E> addVertex();

    /**
     * Removes the specified edge from the graph.
     *
     * @param edge
     */
    void removeEdge(AtlasEdge<V, E> edge);

    /**
     * Removes the specified vertex from the graph.
     *
     * @param vertex
     */
    void removeVertex(AtlasVertex<V, E> vertex);

    /**
     * Retrieves the edge with the specified id.    As an optimization, a non-null Edge may be
     * returned by some implementations if the Edge does not exist.  In that case,
     * you can call {@link AtlasElement#exists()} to determine whether the vertex
     * exists.  This allows the retrieval of the Edge information to be deferred
     * or in come cases avoided altogether in implementations where that might
     * be an expensive operation.
     *
     * @param edgeId
     * @return
     */
    AtlasEdge<V, E> getEdge(String edgeId);

    /**
     * Gets all the edges in the graph.
     * @return
     */
    Iterable<AtlasEdge<V, E>> getEdges();

    /**
     * Gets all the vertices in the graph.
     * @return
     */
    Iterable<AtlasVertex<V, E>> getVertices();

    /**
     * Gets the vertex with the specified id.  As an optimization, a non-null vertex may be
     * returned by some implementations if the Vertex does not exist.  In that case,
     * you can call {@link AtlasElement#exists()} to determine whether the vertex
     * exists.  This allows the retrieval of the Vertex information to be deferred
     * or in come cases avoided altogether in implementations where that might
     * be an expensive operation.
     *
     * @param vertexId
     * @return
     */
    AtlasVertex<V, E> getVertex(String vertexId);

    /**
     * Gets the names of the indexes on edges
     * type.
     *
     * @param type
     * @return
     */
    Set<String> getEdgeIndexKeys();


    /**
     * Gets the names of the indexes on vertices.
     * type.
     *
     * @param type
     * @return
     */
    Set<String> getVertexIndexKeys();


    /**
     * Finds the vertices where the given property key
     * has the specified value.  For multi-valued properties,
     * finds the vertices where the value list contains
     * the specified value.
     *
     * @param key
     * @param value
     * @return
     */
    Iterable<AtlasVertex<V, E>> getVertices(String key, Object value);

    /**
     * Creates a graph query.
     *
     * @return
     */
    AtlasGraphQuery<V, E> query();

    /**
     * Creates an index query.
     *
     * @param indexName index name
     * @param queryString the query
     *
     * @see <a
     * href="https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html">
     * Elastic Search Reference</a> for query syntax
     */
    AtlasIndexQuery<V, E> indexQuery(String indexName, String queryString);

    /**
     * Gets the management object associated with this graph and opens a transaction
     * for changes that are made.
     * @return
     */
    AtlasGraphManagement getManagementSystem();

    /**
     * Commits changes made to the graph in the current transaction.
     */
    void commit();

    /**
     * Rolls back changes made to the graph in the current transaction.
     */
    void rollback();

    /**
     * Unloads and releases any resources associated with the graph.
     */
    void shutdown();

    /**
     * Deletes all data in the graph.  May or may not delete
     * the indices, depending on the what the underlying graph supports.
     *
     * For testing only.
     *
     */
    void clear();

    /**
     * Converts the graph to gson and writes it to the specified stream.
     *
     * @param os
     * @throws IOException
     */
    void exportToGson(OutputStream os) throws IOException;

    /**
     * Get an instance of the script engine to execute Gremlin queries
     *
     * @return script engine to execute Gremlin queries
     */
    ScriptEngine getGremlinScriptEngine() throws AtlasBaseException;

    /**
     * Release an instance of the script engine obtained with getGremlinScriptEngine()
     *
     * @param scriptEngine: ScriptEngine to release
     */
    void releaseGremlinScriptEngine(ScriptEngine scriptEngine);

    /**
     * Executes a Gremlin script, returns an object with the result.
     *
     * @param gremlinQuery
     * @param isPath whether this is a path query
     *
     * @return the result from executing the script
     *
     * @throws ScriptException
     */
    Object executeGremlinScript(String query, boolean isPath) throws AtlasBaseException;

    /**
     * Executes a Gremlin script using a ScriptEngineManager provided by consumer, returns an object with the result.
     * This is useful for scenarios where an operation executes large number of queries.
     *
     * @param bindings: Update bindings with Graph instance for ScriptEngine that is initilized externally.
     * @param query
     * @param isPath whether this is a path query
     *
     * @return the result from executing the script
     *
     * @throws ScriptException
     */
    Object executeGremlinScript(String query, Map<String,Object> bindings, boolean isPath) throws AtlasBaseException;


    /**
     * Convenience method to check whether the given property is
     * a multi-property.
     *
     * @param name
     * @return
     */
    boolean isMultiProperty(String name);
}
