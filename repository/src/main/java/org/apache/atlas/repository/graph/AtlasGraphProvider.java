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

package org.apache.atlas.repository.graph;

import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.GraphInitializationException;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.GraphDatabase;
import org.apache.atlas.repository.graphdb.GremlinVersion;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.util.AtlasRepositoryConfiguration;

import java.util.Collections;

/**
 * Provides access to the AtlasGraph
 *
 */
public class AtlasGraphProvider implements IAtlasGraphProvider {


    private static volatile GraphDatabase<?,?> graphDb_;

    public AtlasGraphProvider() {

   }

    /**
     * Gets the instance of the AtlasGraph to use for the current tenant.  Different instances will
     * be returned for different tenants.
     *
     * @return
     * @throws RepositoryException
     */
    public static <V, E> AtlasGraph<V, E> getGraphInstance() {
      GraphDatabase<?,?> db = getGraphDatabase();

      initializeGraph(db);
      AtlasGraph<?, ?> graph = db.getUserGraph();
      return (AtlasGraph<V, E>) graph;
    }

    private static <V, E> GraphDatabase<?,?> getGraphDatabase() {

        try {
            if (graphDb_ == null) {
                synchronized(AtlasGraphProvider.class) {
                    if(graphDb_ == null) {
                        Class implClass = AtlasRepositoryConfiguration.getGraphDatabaseImpl();
                        graphDb_ = (GraphDatabase<V, E>) implClass.newInstance();
                    }
                }
            }
            return graphDb_;
        }
        catch (IllegalAccessException | InstantiationException e) {
            throw new GraphInitializationException("Error initializing graph database", e);
        }
    }

    private static void initializeGraph(GraphDatabase<?, ?> graphDb) {

        //TODO (MT-support): pass tenant-id to initialize method.
        graphDb.initialize(Collections.<String, String>emptyMap());
    }


    @VisibleForTesting
    public static void initializeTestGraph() {
        getGraphDatabase().initializeTestGraph();
    }

    @VisibleForTesting
    public static void cleanup() {
        getGraphDatabase().cleanup(true);
    }

    @Override
    public AtlasGraph get() throws GraphInitializationException {
        return getGraphInstance();
    }

    public static boolean isGraphScanAllowed() {
        return getGraphDatabase().isGraphScanAllowed();
    }

    public static GremlinVersion getSupportedGremlinVersion() {
        return getGraphDatabase().getSupportedGremlinVersion();
    }

    /**
     * This method is used in the generation of queries.  It is used to
     * convert property values from the value that is stored in the graph
     * to the value/type that the user expects to get back.
     *
     * @param expr - gremlin expr that represents the persistent property value
     * @param attrType
     * @return
     */
    public static GroovyExpression generatePersisentToLogicalConversionExpression(GroovyExpression expr, IDataType attrType) {
        return getGraphDatabase().generatePersisentToLogicalConversionExpression(expr, attrType);
    }


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
    public static boolean isPropertyValueConversionNeeded(IDataType attrType) {
        return getGraphDatabase().isPropertyValueConversionNeeded(attrType);
    }

    /**
     * Whether or not an initial predicate needs to be added to gremlin queries
     * in order for them to run successfully.  This is needed for some graph database where
     * graph scans are disabled.
     * @return
     */
    public static boolean requiresInitialIndexedPredicate() {
        return getGraphDatabase().requiresInitialIndexedPredicate();
    }

    /**
     * Some graph database backends have graph scans disabled.  In order to execute some queries there,
     * an initial 'dummy' predicate needs to be added to gremlin queries so that the first
     * condition uses an index.
     *
     * @return
     */
    public static GroovyExpression getInitialIndexedPredicate(GroovyExpression parent) {
        return getGraphDatabase().getInitialIndexedPredicate(parent);
    }

    /**
     * As an optimization, a graph database implementation may want to retrieve additional
     * information about the query results.  For example, in the IBM Graph implementation,
     * this changes the query to return both matching vertices and their outgoing edges to
     * avoid the need to make an extra REST API call to look up those edges.  For implementations
     * that do not require any kind of transform, an empty String should be returned.
     */
    public static GroovyExpression addOutputTransformationPredicate(GroovyExpression expr, boolean isSelect, boolean isPath) {
        return getGraphDatabase().addOutputTransformationPredicate(expr, isSelect, isPath);
    }


}
