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

import java.util.List;

import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.ibmgraph.gremlin.step.GremlinStep;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;

/**
 * Interface that controls how tenants get assigned to graphs and how
 * that affects various graph operations.
 */
public interface TenantGraphStrategy {

    /**
     * Gets the name of the graph that should be used for the
     * specified tenant.
     *
     */
    String getGraphName(String tenantId);

    /**
     * Whether or not graphs created for this strategy need to be explicitly
     * initialized through the TenantRegistrationListener mechanism.  This
     * is only needed in cases where Atlas does not automatically do
     * the required initialization at startup.
     * @return
     */
    boolean isGraphInitializationNeeded();

    /**
     * Gets the graph traversal expression to use when executing queries and
     * creating vertices.  The expression can assume that the 'graph' variable
     * has been pre-defined to refer to the graph being used.
     *
     * @param tenantId the tenant id to be used during the traversal.
     *
     * @return the traversal expression.  Must be non-null.
     */
    GroovyExpression getGraphTraversalExpression(GroovyExpression tenantId);

   /**
    * Gets the graph traversal expression to use when executing queries and
    * creating vertices.  The expression can assume that the 'graph' variable
    * has been pre-defined to refer to the graph being used.
    *
    * @param tenantId the tenant id to be used during the traversal
    *
    * @return the traversal expression.  Must be non-null.
    */
    GroovyExpression getGraphTraversalExpression(String tenantId);

    /**
     * Gets the id of the user tenant to use for the current thread.
     *
     * @return
     */
    String getUserTenantId();

    /**
     * Returns the id of the shared tenant to use for the current thread.
     *
     * @return
     */
    String getSharedTenantId();

    /**
     * This is called whenever a new vertex is created.  It updates the vertex with
     * any properties that are needed for this TenantGraphStrategy.
     *
     * @param vertex
     * @param graphId
     * @param tenantId
     */
    void updateNewVertexIfNeeded(IBMGraphVertex vertex, String graphId, String tenantId);

    /**
     * This is called whenever an index query is executed.  It updates the index query
     * to include any additional conditions needed by this strategy to restrict
     * the results to only those associated with the given tenant.
     *
     * @return the updated query string
     */
    String updateIndexQueryIfNeeded(String queryString, String graphId, String tenantId);

    /**
     * Updates <code>mgmt</code> with any base property keys and indices that are specific to this
     * TenantGraphStrategy.
     *
     * @param mgmt instance of IBMGraphManagement that the indices should be added to.
     */
    void addAdditionalBaseKeysAndIndicesIfNeeded(IBMGraphMetadata metadata, IBMGraphManagement mgmt);

    /**
     * Gets the additional index keys, if any, that should be added to all indices
     * created by Atlas.  If there are none, an empty list must be returned.
     *
     * @param mgmt instance of IBMGraphManagement that can be used to create/retrieve
     *    property keys that are needed to support this strategy.
     *
     * @return the result.  Must be non-null.
     *
     */
    List<AtlasPropertyKey> getAdditionalIndexKeys(IBMGraphManagement mgmt);

    /**
     * Gets a predicate that matches all vertices for the current tenant.
     *
     * @return the predicate to use, or null of none is needed.
     */
    GremlinStep getInitialIndexedPredicateExpr();

}
