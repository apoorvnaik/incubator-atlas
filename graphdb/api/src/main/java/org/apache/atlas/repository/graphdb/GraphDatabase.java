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

import org.apache.atlas.GraphInitializationException;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.typesystem.types.IDataType;

import java.util.Map;

/**
 * Represents a graph database.
 *
 * @param <V> vertex class used by the graph database
 * @param <E> edge class used by the graph database
 */
public interface GraphDatabase<V, E> {

    /**
     * Returns whether the graph has been loaded.
     * @return
     */
    boolean isGraphLoaded();

    /**
     * Gets the graph to be used for storing shared data such as type definitions.  This
     * may or may not be the same as the user graph, depending on the implementation.
     * Different backends are free to implement support for multi-tenancy in their own
     * way.
     *
     */
    AtlasGraph<V, E> getSharedGraph() throws GraphInitializationException;

    /**
     * Gets the graph to be used for storing tenant-specific data such as entity definitions.  This
     * may or may not be the same as the shared graph, depending on the implementation.
     * Different backends are free to implement support for multi-tenancy in their own
     * way.
     */
    AtlasGraph<V, E> getUserGraph() throws GraphInitializationException;

    /**
     * Sets things up so that getGraph() will return a graph that can be used for running
     * tests.
     */
    void initializeTestGraph();

    /**
     * Removes the test graph that was created.
     */
    void cleanup(boolean deleteGraph);

    /**
     * Initializes the graph with the given parameters.  This lets
     * the graph database know what tenant is being used.
     */
    void initialize(Map<String, String> initParams) throws GraphInitializationException;

    /**
     * Whether the given graph backend allows full graph scans.
     * @return
     */
    boolean isGraphScanAllowed();

    /**
     * Gets the version of Gremlin that this graph uses.
     *
     * @return
     */
    GremlinVersion getSupportedGremlinVersion();

    //the following methods insulate Atlas from the details
    //of the interaction with Gremlin

    /**
     * This method is used in the generation of queries.  It is used to
     * convert property values from the value that is stored in the graph
     * to the value/type that the user expects to get back.
     *
     * @param valueExpr - gremlin expr that represents the persistent property value
     * @param type
     * @return
     */
    GroovyExpression generatePersisentToLogicalConversionExpression(GroovyExpression valueExpr, IDataType<?> type);

    /**
     * Indicates whether or not stored values with the specified type need to be converted
     * within generated gremlin queries before they can be compared with literal values.
     * As an example, a graph database might choose to store Long values as Strings or
     * List values as a delimited list.  In this case, the generated gremlin needs to
     * convert the stored property value prior to comparing it a literal.  In this returns
     * true, @code{generatePersisentToLogicalConversionExpression} is used to generate a
     * gremlin expression with the converted value.  In addition, this cause the gremlin
     * 'filter' step to be used to compare the values instead of a 'has' step.
     */
    boolean isPropertyValueConversionNeeded(IDataType<?> type);

    /**
     * Whether or not an initial predicate needs to be added to gremlin queries
     * in order for them to run successfully.  This is needed for some graph database where
     * graph scans are disabled.
     * @return
     */
    boolean requiresInitialIndexedPredicate();

    /**
     * Some graph database backends have graph scans disabled.  In order to execute some queries there,
     * an initial 'dummy' predicate needs to be added to gremlin queries so that the first
     * condition uses an index.
     *
     * @return
     */
    GroovyExpression getInitialIndexedPredicate(GroovyExpression parent);

    /**
     * As an optimization, a graph database implementation may want to retrieve additional
     * information about the query results.  For example, in the IBM Graph implementation,
     * this changes the query to return both matching vertices and their outgoing edges to
     * avoid the need to make an extra REST API call to look up those edges.  For implementations
     * that do not require any kind of transform, an empty String should be returned.
     */
    GroovyExpression addOutputTransformationPredicate(GroovyExpression expr, boolean isSelect, boolean isPath);
}
