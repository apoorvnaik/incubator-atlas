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

package org.apache.atlas.ibmgraph.gremlin.step;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.atlas.groovy.FunctionCallExpression;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.TraversalStepType;
import org.apache.atlas.ibmgraph.IBMGraphGraph;
import org.apache.atlas.ibmgraph.IBMGraphVertex;
import org.apache.atlas.ibmgraph.gremlin.expr.GraphTraversalExpression;

/**
 * Gremlin step which returns elements which match any of the
 * provided child traversals.
 */
public class OrStep extends GremlinStep {

    private List<GraphTraversalExpression> childTraversals_;

    public OrStep() {
        childTraversals_ = new ArrayList<GraphTraversalExpression>();
    }

    public void addChildTraversal(GraphTraversalExpression expr) {
        childTraversals_.add(expr);
    }

    /**
     * Constructor
     *
     * @param subTraversals
     */
    public OrStep(GraphTraversalExpression... subTraversals) {
        childTraversals_ = Arrays.asList(subTraversals);
    }

    /**
     * Constructor
     *
     * @param oneStepTraversals
     */
    public OrStep(GremlinStep... oneStepTraversals) {
        childTraversals_ = new ArrayList<>(oneStepTraversals.length);
        for(GremlinStep step : oneStepTraversals) {
            GraphTraversalExpression expr = new GraphTraversalExpression();
            expr.addStep(step);
            childTraversals_.add(expr);
        }
    }


    @Override
    public GremlinEvaluationResult evaluateOnCache(IBMGraphGraph graph) {

        //combines the result of executing all of the child graph
        //traversals using a set union operation.  Note that
        //Sets.union was avoided here because the javadoc says
        //nesting Set.union calls performs badly.
        Set<IBMGraphVertex> result = new HashSet<IBMGraphVertex>();
        for(GraphTraversalExpression expr : childTraversals_) {
            GremlinEvaluationResult exprResult = expr.evaluateOnCache(graph);
            result.addAll(exprResult.getMatchingVertices());
        }
        return new GremlinEvaluationResult(result);
    }


    @Override
    public GroovyExpression generateGroovy(GroovyExpression parent) {

    	FunctionCallExpression result = new FunctionCallExpression(TraversalStepType.FILTER, parent, "or");
        Iterator<GraphTraversalExpression> it = childTraversals_.iterator();
        while(it.hasNext()) {
            GraphTraversalExpression expr = it.next();
            result.addArgument(expr.generateGroovy());
        }
        return result;
    }

    @Override
    public boolean supportsEvaluation() {
        for(GraphTraversalExpression expr : childTraversals_) {
            if(! expr.supportsEvaluation()) {
                return false;
            }
        }
        return true;
    }


    @Override
    public boolean matches(IBMGraphVertex vertex) {
        for(GraphTraversalExpression expr : childTraversals_) {
            if(expr.matches(vertex)) {
                return true;
            }
        }
        return false;
    }

}
