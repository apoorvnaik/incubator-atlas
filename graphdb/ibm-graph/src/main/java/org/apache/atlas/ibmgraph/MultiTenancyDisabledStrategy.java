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

import java.util.Collections;
import java.util.List;

import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.ibmgraph.api.GraphDatabaseConfiguration;
import org.apache.atlas.ibmgraph.gremlin.expr.DefaultGraphTraversalExpression;
import org.apache.atlas.ibmgraph.gremlin.step.GremlinStep;
import org.apache.atlas.ibmgraph.gremlin.step.HasStep;
import org.apache.atlas.repository.graphdb.AtlasCardinality;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;

/**
 * TenantGraphStrategy that uses a separate graph for each tenant.  Each tenant
 * can have its own types and data.  Both of these go in the graph for the tenant.
 * The types and data for a given tenant go into a graph which has the same name
 * as the tenant.  If a tenant id is not specified in the Atlas request, the
 * graph name specified in credentials.json will be used as the tenant and graph name.
 */
public class MultiTenancyDisabledStrategy implements TenantGraphStrategy {

    private static final String IS_GRAPHDB_VERTEX = "__is_graphdb_vertex";

    public MultiTenancyDisabledStrategy() throws Exception{

    }
    @Override
    public String getGraphName(String tenantId) {
        return GraphDatabaseConfiguration.INSTANCE.getGraphName();
    }

    @Override
    public boolean isGraphInitializationNeeded() {
        return false;
    }

    @Override
    public GroovyExpression getGraphTraversalExpression(String tenantId) {
        return new DefaultGraphTraversalExpression();
    }

    @Override
    public GroovyExpression getGraphTraversalExpression(GroovyExpression tenantId) {
        return new DefaultGraphTraversalExpression();
    }

    @Override
    public String getUserTenantId() {
        return GraphDatabaseConfiguration.INSTANCE.getGraphName();
    }

    @Override
    public String getSharedTenantId() {
        return GraphDatabaseConfiguration.INSTANCE.getGraphName();
    }

    @Override
    public void updateNewVertexIfNeeded(IBMGraphVertex vertex, String graphId, String tenantId) {
        vertex.setProperty(IS_GRAPHDB_VERTEX, true);
    }

    @Override
    public String updateIndexQueryIfNeeded(String queryString, String graphId, String tenantId) {
        //nothing to do
        return queryString;
    }

    @Override
    public void addAdditionalBaseKeysAndIndicesIfNeeded(IBMGraphMetadata graph, IBMGraphManagement mgmt) {
        if(! graph.getExistingIndices().containsKey(IS_GRAPHDB_VERTEX)) {
            AtlasPropertyKey key =mgmt. makePropertyKey(IS_GRAPHDB_VERTEX, Boolean.class, AtlasCardinality.SINGLE);
            mgmt.createCompositeIndex_(IS_GRAPHDB_VERTEX, Collections.singletonList(key), false);
        }
    }

    @Override
    public List<AtlasPropertyKey> getAdditionalIndexKeys(IBMGraphManagement mgmt) {
        //nothing to do
        return Collections.emptyList();
    }

    @Override
    public GremlinStep getInitialIndexedPredicateExpr() {
        return new HasStep(IS_GRAPHDB_VERTEX, true);
    }

}
