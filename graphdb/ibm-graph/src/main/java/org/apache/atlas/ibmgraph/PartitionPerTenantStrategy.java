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
import org.apache.atlas.groovy.LiteralExpression;
import org.apache.atlas.ibmgraph.api.GraphDatabaseConfiguration;
import org.apache.atlas.ibmgraph.gremlin.expr.TraversalSourceExpression;
import org.apache.atlas.ibmgraph.gremlin.step.GremlinStep;
import org.apache.atlas.ibmgraph.http.IBMGraphRequestContext;
import org.apache.atlas.repository.graphdb.AtlasCardinality;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
/**
 * TenantGraphStrategy that uses one partitioned graph for all of the tenants.
 * There is a global tenant that all of the types are associated with.  Tenants
 * cannot have their own types.  Any incoming requests that do not have a tenant
 * associated with them are associated with the global tenant.  The name of the graph
 * used is specified in credentials.json.
 */
public class PartitionPerTenantStrategy implements TenantGraphStrategy {

    private static final String PARTITION_PROPERTY_KEY = "__tenant_id";
    private static final String GLOBAL_TENANT_ID = "__global__";

    public PartitionPerTenantStrategy () throws Exception{

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
        return getGraphTraversalExpression(new LiteralExpression(tenantId));
    }

    @Override
    public GroovyExpression getGraphTraversalExpression(GroovyExpression tenantId) {
        return new TraversalSourceExpression(tenantId, PARTITION_PROPERTY_KEY);
    }

    @Override
    public String getUserTenantId() {
       String tenantId = IBMGraphRequestContext.get().getTenantId();
       if(tenantId == null) {
           tenantId = GLOBAL_TENANT_ID;
       }
       return tenantId;
    }

    @Override
    public String getSharedTenantId() {
        return GLOBAL_TENANT_ID;
    }

    @Override
    public void updateNewVertexIfNeeded(IBMGraphVertex vertex, String graphId, String tenantId) {
        vertex.setProperty(PARTITION_PROPERTY_KEY, tenantId);

    }

    @Override
    public String updateIndexQueryIfNeeded(String queryString, String graphId, String tenantId) {
        return String.format("v.\"%s\":\"%s\" AND %s", PARTITION_PROPERTY_KEY, tenantId, queryString);

    }

    @Override
    public void addAdditionalBaseKeysAndIndicesIfNeeded(IBMGraphMetadata metadata, IBMGraphManagement mgmt) {
        if(! metadata.getExistingPropertyKeys().containsKey(PARTITION_PROPERTY_KEY)) {
            AtlasPropertyKey key = mgmt.makePropertyKey(PARTITION_PROPERTY_KEY, String.class, AtlasCardinality.SINGLE);
            mgmt.createCompositeIndex_(PARTITION_PROPERTY_KEY, Collections.singletonList(key), false);
        }

    }

    @Override
    public List<AtlasPropertyKey> getAdditionalIndexKeys(IBMGraphManagement mgmt) {
        return Collections.singletonList(mgmt.getPropertyKey(PARTITION_PROPERTY_KEY));
    }

    @Override
    public GremlinStep getInitialIndexedPredicateExpr() {
        //none needed, tenant id predicate is always added
        return null;
    }
}
