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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.GroovyGenerationContext;
import org.apache.atlas.groovy.StatementListExpression;
import org.apache.atlas.ibmgraph.api.IGraphDatabaseClient;
import org.apache.atlas.ibmgraph.api.json.JsonVertexData;
import org.apache.atlas.ibmgraph.gremlin.expr.GraphTraversalExpression;
import org.apache.atlas.ibmgraph.gremlin.expr.TransformQueryResultExpression;
import org.apache.atlas.ibmgraph.gremlin.step.GremlinEvaluationResult;
import org.apache.atlas.ibmgraph.gremlin.step.GremlinStep;
import org.apache.atlas.ibmgraph.gremlin.step.HasStep;
import org.apache.atlas.ibmgraph.gremlin.step.OrStep;
import org.apache.atlas.ibmgraph.gremlin.step.WithinStep;
import org.apache.atlas.ibmgraph.util.GraphDBUtil;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 * Executes an attribute-based query on the graph.  This class has logic that allows a subset of the queries to
 * be executed without forcing the pending changes to be pushed into the graph.  This can be done for queries that
 * only use "equals" or "within" as the has comparison operator.  For all other queries, we push the changes to the graph first
 * and then execute the query on the graph.
 *
 */
public class IBMGraphGraphQuery implements AtlasGraphQuery<IBMGraphVertex, IBMGraphEdge> {

    private final boolean isChildQuery_;

    private GraphTraversalExpression expr = new GraphTraversalExpression();
    private IBMGraphGraph graph_;

    private static final Logger logger_ = LoggerFactory.getLogger(IBMGraphGraphQuery.class.getName());

    public IBMGraphGraphQuery(IBMGraphGraph graph) {
        this(graph, false);
    }
    public IBMGraphGraphQuery(IBMGraphGraph graph, boolean isChildQuery) {
        graph_ = graph;
        isChildQuery_ = isChildQuery;
        if(! isChildQuery) {
            expr.g().V();
        }
    }

    @Override
    public AtlasGraphQuery<IBMGraphVertex,IBMGraphEdge> has(String property, Object value) {
        expr.has(property, value);
        return this;
    }

    @Override
    public AtlasGraphQuery<IBMGraphVertex,IBMGraphEdge> has(String property, ComparisionOperator op, Object value) {
        expr.has(property, op, value);
        return this;
    }

    @Override
    public AtlasGraphQuery<IBMGraphVertex, IBMGraphEdge> or(List<AtlasGraphQuery<IBMGraphVertex, IBMGraphEdge>> childQueries) {

        OrStep or = new OrStep();
        for(AtlasGraphQuery<IBMGraphVertex,IBMGraphEdge> query : childQueries) {
            if(! query.isChildQuery()) {
                throw new IllegalArgumentException(query + " is not a child query");
            }
            or.addChildTraversal(getTraversal(query));
        }
        expr.addStep(or);
        return this;
    }

    private GraphTraversalExpression getTraversal(AtlasGraphQuery<IBMGraphVertex, IBMGraphEdge> query) {
        return ((IBMGraphGraphQuery)query).getTraversalExpr();
    }

    public GraphTraversalExpression getTraversalExpr() {
        return expr;
    }
    private Set<AtlasVertex<IBMGraphVertex, IBMGraphEdge>> evaluateOnGraphAndFilter() {
        Collection<AtlasVertex<IBMGraphVertex, IBMGraphEdge>> currentGraphResult = evaluateOnGraph();
        Set<AtlasVertex<IBMGraphVertex, IBMGraphEdge>> graphMatches = new HashSet<>();
        //filter out results that no longer match due to property changes
        //(deleted vertices are filtered out as part of the query execution)
        for(AtlasVertex<IBMGraphVertex, IBMGraphEdge> vertex : currentGraphResult) {
            if(expr.matches(vertex.getV())) {
                graphMatches.add(vertex);
            }
        }
        return graphMatches;
    }

    private Collection<AtlasVertex<IBMGraphVertex,IBMGraphEdge>> evaluateOnGraph() {

        GroovyExpression traversalExpression = expr.toList().generateGroovy();
        GroovyExpression transformed = new TransformQueryResultExpression(traversalExpression, false, false);
        StatementListExpression stmt = GraphDBUtil.addDefaultInitialStatements(graph_.getTenantId(), transformed);

        GroovyGenerationContext context = new GroovyGenerationContext();
        stmt.generateGroovy(context);

        StringBuilder query = new StringBuilder();

        query.append(context.getQuery());

        List<JsonVertexData> elements = graph_.getClient().executeGremlin(query.toString(), context.getParameters(), JsonVertexData.class);
        return graph_.wrapVertices(elements, true);
    }

    @Override
    public AtlasGraphQuery createChildQuery() {

        return new IBMGraphGraphQuery(graph_,  true);
    }
    /**
     * Executes an attribute-based vertex query on the graph.  This method has logic that allows certain queries to
     * be executed without forcing the pending changes to be pushed into the graph.  This can be done for queries that
     * only use "equals" or "within" as the has comparison operator.  For all other queries, we push the changes to the graph
     * first and then execute the query on the graph.
     *
     * For queries that support in-memory evaluation, we compute the query result by first executing the query on the graph.
     *  We take those results and filter out vertices that no longer match due to changes that have been applied but not yet pushed
     * into IBM Graph (specifically vertices that have been deleted or who no longer match the query condition due
     * to property value changes).
     *
     * The next step is to find vertices that now match the query due to property value changes.  This is handled
     * by the individual step classes.  Most of the heavy lifting is done by PropertyIndex, which is used to find
     * updated vertices based on property values.
     *
     * The overall query result is the union of the results from the graph and the results from property changes.
     */
    @Override
    public Iterable<AtlasVertex<IBMGraphVertex, IBMGraphEdge>> vertices() {

        if(! expr.supportsEvaluation()) {
            //generate query, force push of changes into graph
            graph_.applyPendingChanges();
            Collection result =  evaluateOnGraph();
            return result;
        }
        else {
            Set<AtlasVertex<IBMGraphVertex, IBMGraphEdge>> graphMatches = evaluateOnGraphAndFilter();
            GremlinEvaluationResult cacheResult = expr.evaluateOnCache(graph_);
            Set<IBMGraphVertex> uncommittedMatches =  cacheResult.getMatchingVertices();
            return Sets.union(graphMatches, uncommittedMatches);
        }
    }

    private IGraphDatabaseClient getClient() {
        return graph_.getClient();
    }

    @Override
    public AtlasGraphQuery<IBMGraphVertex, IBMGraphEdge> in(String propertyKey, Collection<? extends Object> values) {
        if(values.size() > 1) {
            expr.addStep(new WithinStep(propertyKey, values));
        }
        if(values.size() == 1) {
            expr.addStep(new HasStep(propertyKey, values.iterator().next()));
        }
        return this;
    }

    @Override
    public AtlasGraphQuery<IBMGraphVertex, IBMGraphEdge> addConditionsFrom(AtlasGraphQuery<IBMGraphVertex,IBMGraphEdge> child) {
        expr.addSteps(((IBMGraphGraphQuery)child).getSteps());
        return this;
    }

    public List<GremlinStep> getSteps() {
        return expr.getSteps();
    }
    @Override
    public boolean isChildQuery() {
        return isChildQuery_;
    }


}
