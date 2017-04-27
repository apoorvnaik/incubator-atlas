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

import java.util.Collection;
import java.util.Iterator;

import org.apache.atlas.ibmgraph.api.GraphDatabaseClient;
import org.apache.atlas.ibmgraph.api.GraphDatabaseConfiguration;
import org.apache.atlas.ibmgraph.api.IGraphDatabaseClient;
import org.apache.atlas.ibmgraph.api.json.JsonIndexQueryResult;
import org.apache.atlas.ibmgraph.gremlin.GremlinQuery;
import org.apache.atlas.ibmgraph.gremlin.QueryGenerationResult;
import org.apache.atlas.ibmgraph.gremlin.expr.IndexQueryExpression;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;

import com.google.common.collect.Collections2;

public class IBMGraphIndexQuery implements AtlasIndexQuery<IBMGraphVertex, IBMGraphEdge> {

    private IBMGraphGraph graph_;
    private String queryString;
    private String indexName;

    public IBMGraphIndexQuery(IBMGraphGraph graph, String indexName, String queryString) {
        graph_ = graph;
        this.indexName = indexName;
        //add tenant condition to query string
        this.queryString = GraphDatabaseConfiguration.INSTANCE.getTenantGraphStrategy().updateIndexQueryIfNeeded(queryString, graph.getGraphId(), graph.getTenantId());
    }
    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasIndexQuery#vertices()
     */
    @Override
    public Iterator<AtlasIndexQuery.Result<IBMGraphVertex, IBMGraphEdge>> vertices() {
        IndexQueryExpression expr = new IndexQueryExpression(this.indexName, this.queryString);
        GremlinQuery query = new GremlinQuery(expr);

        QueryGenerationResult generatedQuery = query.generateGremlin();
        IGraphDatabaseClient client = getClient();
        Collection<JsonIndexQueryResult> jsonResult = client.executeGremlin(generatedQuery.getQuery(), generatedQuery.getParameters(), JsonIndexQueryResult.class);
        Collection<AtlasIndexQuery.Result<IBMGraphVertex, IBMGraphEdge>> result = Collections2.transform(jsonResult, graph_.getIndexQueryResultWrapperFunction());
        return result.iterator();
    }


    private IGraphDatabaseClient getClient() {
        return graph_.getClient();
    }
}
