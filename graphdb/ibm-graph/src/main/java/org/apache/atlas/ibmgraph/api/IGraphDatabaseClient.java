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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.atlas.AtlasException;
import org.apache.atlas.ibmgraph.api.json.Index;
import org.apache.atlas.ibmgraph.api.json.JsonEdge;
import org.apache.atlas.ibmgraph.api.json.Schema;
import org.apache.atlas.ibmgraph.exception.GraphDatabaseException;
import org.apache.atlas.ibmgraph.http.IHttpRetryStrategy;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;

import com.google.gson.Gson;

/**
 * Client for interacting directly with IBM Graph via its REST API.
 */
public interface IGraphDatabaseClient {

    /**
     * Retrieves the edge with the given id from IBM Graph.
     */
    JsonEdge getEdge(String edgeId);

    /**
     * Retrieves the schema from IBM Graph.
     *
     * @return
     * @throws GraphDatabaseException
     */
    Schema getSchema() throws GraphDatabaseException;

    /**
     * Adds the indices and property keys to IBM Graph in the given Schema to
     * IBM Graph.
     *
     * @param schema
     * @param strategy
     *            the http retry strategy to use
     * @return
     * @throws GraphDatabaseException
     */
    Schema updateSchema(Schema schema, IHttpRetryStrategy strategy) throws GraphDatabaseException;

    /**
     * Retrieves edges incident to the given vertex.
     *
     * @param vertexId
     * @param dir
     * @param label
     *            , ignored if null
     * @return
     * @throws GraphDatabaseException
     */
    Collection<JsonEdge> getIncidentEdges(String vertexId, AtlasEdgeDirection dir, String label)
            throws GraphDatabaseException;

    /**
     * Executes a parameterized gremlin query.
     *
     * @param query
     *            the query string
     * @param bindings
     *            the bindings to use for the parameter values
     * @param clazz
     *            the type of object that should returned by the query
     *
     * @return the query result
     * @throws GraphDatabaseException
     */
    <T> List<T> executeGremlin(String query, Map<String, Object> bindings, Class<T> clazz);


    /**
     * Executes a parameterless gremlin query.
     *
     * @param query
     *            the query string
     * @param clazz
     *            the type of object that should returned by the query
     *
     * @return the query result
     * @throws GraphDatabaseException
     */
    <T> List<T> executeGremlin(String query, Class<T> clazz) throws GraphDatabaseException;

    /**
     * Executes a parameterized gremlin query using a specific Gson
     * configuration to serialize the binding values.
     *
     * @param query
     *            the query string
     * @param bindings
     *            the bindings to use for the parameter values
     * @param clazz
     *            the type of object that should returned by the query
     * @param serializer
     *            Gson instance to use
     *
     * @return the query result
     * @throws GraphDatabaseException
     */
    <T> List<T> executeGremlin(String query, Map<String, Object> bindings, Class<T> clazz, Gson serializer)
            throws GraphDatabaseException;

    /**
     * Fetches information about the given index from IBM Graph.
     * @param indexName
     * @return
     */
    Index getIndex(String indexName);

    /**
     * Deletes the specified index from IBM Graph.
     *
     * @param indexName
     * @return
     */
    void deleteIndex(String indexName);

    /**
     * Runs the index activation script.
     *
     * @param name
     * @param props
     * @return
     */
    boolean activateIndex(String name, Collection<String> props);

    /**
     * Creates graph with given id if it does not exist yet returns true if
     * graph was created in ibm graph db. false if graph already exist.
     *
     * @param id
     * @param url
     */
    boolean createGraph() throws AtlasException;

    /**
     * Deletes the graph with the given graphId from IBM Graph.
     *
     * @param graphId
     */
    void deleteGraph(String graphId);

    /**
     * Gets the status of the index with the given name.
     *
     * @param name the name of the index
     * @param expectedProperties names property keys that should be in the index
     * @return
     */
    IndexStatus getOverallStatus(String name, Collection<String> expectedProperties);

    /**
     * Determines the list of property keys currently within the given index.
     *
     * @param indexName
     * @return
     */
    Collection<String> getPropertyKeysInIndex(String indexName);
}
