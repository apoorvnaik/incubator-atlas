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

package org.apache.atlas.ibmgraph.gremlin.expr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.ibmgraph.IBMGraphGraph;
import org.apache.atlas.ibmgraph.IBMGraphVertex;
import org.apache.atlas.ibmgraph.api.GraphDatabaseConfiguration;
import org.apache.atlas.ibmgraph.gremlin.step.GraphStep;
import org.apache.atlas.ibmgraph.gremlin.step.GremlinEvaluationResult;
import org.apache.atlas.ibmgraph.gremlin.step.GremlinStep;
import org.apache.atlas.ibmgraph.gremlin.step.HasStep;
import org.apache.atlas.ibmgraph.gremlin.step.OrStep;
import org.apache.atlas.ibmgraph.gremlin.step.ToListStep;
import org.apache.atlas.ibmgraph.gremlin.step.VertexStep;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery.ComparisionOperator;

/**
 * Gremlin expression representing a graph traversal.  These are used
 * to create gremlin queries.
 */
public class GraphTraversalExpression implements EvaluableGremlinExpression {

    //the steps in the graph traversal
    private List<GremlinStep> steps_ = new ArrayList<GremlinStep>();

    /**
     * Adds a step to the graph traversal
     *
     * @param step the step to add
     */
    public void addStep(GremlinStep step) {
        if(getLastStepAdded() instanceof VertexStep) {
            if(step instanceof OrStep) {
                //IBMGraph does not support queries that start with g.V().or() or g.V().and().  Add in
                // a dummy step before it so that the query will run
                GremlinStep initialExpr = GraphDatabaseConfiguration.INSTANCE.getTenantGraphStrategy().getInitialIndexedPredicateExpr();
                if(initialExpr != null) {
                    steps_.add(initialExpr);
                }
            }
        }
        steps_.add(step);
    }

    private GremlinStep getLastStepAdded() {
        if(steps_.isEmpty()) {
            return null;
        }
        return steps_.get(steps_.size() - 1);
    }
    /**
     * Adds steps to the graph traversal
     *
     * @param steps the steps to add
     */
    public void addSteps(List<? extends GremlinStep> steps) {
        steps_.addAll(steps);
    }


    /**
     * Convenience method that adds a graph ("g") step to the graph traversal
     *
     * @return this graph traversal expression
     */
    public GraphTraversalExpression g() {
        addStep(new GraphStep());
        return this;
    }


    /**
     * Convenience method that adds a Vertex ("V") step to the graph traversal
     * that retrieves the vertex with the given vertexId
     *
     * @param vertexId the id of the vertex to retrieve
     *
     * @return this graph traversal expression
     */
    public GraphTraversalExpression V(String vertexId) {
        addStep(new VertexStep(vertexId));
        return this;
    }

    /**
     * Convenience method that adds a Vertex ("V") step to the graph traversal.
     *
     * @return this graph traversal expression
     */
    public GraphTraversalExpression V() {
        addStep(new VertexStep());
        return this;
    }


    /**
     * Convenience method that adds a has step to the graph traversal.
     *
     * @param property the property to check for the existence of.
     * @param value the value of the property that is required.
     *
     * @return this graph traversal expression
     */
    public GraphTraversalExpression has(String property, Object value) {
        addStep(new HasStep(property, value));
        return this;
    }
    public GraphTraversalExpression has(String property, ComparisionOperator op, Object value) {
        addStep(new HasStep(property, op, value));
        return this;
    }


    /**
     * Convenience method that adds a has step to the graph traversal.
     *
     * @param property the property to check for the existence of.
     *
     * @return this graph traversal expression
     */
    public GraphTraversalExpression has(String property) {
        addStep(new HasStep(property));
        return this;
    }

    public GraphTraversalExpression toList() {
        addStep(ToListStep.INSTANCE);
        return this;
    }

    public GroovyExpression generateGroovy() {
        GroovyExpression result = null;
        for(GremlinStep step : steps_) {
        	result = step.generateGroovy(result);
        }
        return result;
    }

    @Override
    public boolean supportsEvaluation() {

        for(GremlinStep step : steps_) {
            if(! step.supportsEvaluation()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public GremlinEvaluationResult evaluateOnCache(IBMGraphGraph graph) {

        GremlinEvaluationResult overallResult = null;
        for(GremlinStep step : steps_) {

            GremlinEvaluationResult stepResult = step.evaluateOnCache(graph);

            if(overallResult  == null) {
                overallResult = stepResult;
            }
            else {
                overallResult = overallResult.intersectWith(stepResult);
            }
        }
        return overallResult;
    }

    @Override
    public boolean matches(IBMGraphVertex vertex) {

        for(GremlinStep step : steps_) {
            if(! step.matches(vertex)) {
                return false;
            }
        }
        return true;
    }

    public List<GremlinStep> getSteps() {
        return Collections.unmodifiableList(steps_);
    }

    public void prependStep(GremlinStep step) {
        steps_.add(0, step);
    }
}
