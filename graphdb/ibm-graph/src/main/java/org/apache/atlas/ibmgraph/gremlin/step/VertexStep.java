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

import org.apache.atlas.groovy.FunctionCallExpression;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.LiteralExpression;
import org.apache.atlas.groovy.TraversalStepType;
import org.apache.atlas.ibmgraph.IBMGraphGraph;
import org.apache.atlas.ibmgraph.IBMGraphVertex;

/**
 * Gremlin graph traversal step that retrieves either all the vertices or
 * the vertex with the given id.
 */
public class VertexStep extends GremlinStep {

    private String optVertexId_;


    public VertexStep() {

    }

    public VertexStep(String vertexId) {
        optVertexId_ = vertexId;
    }


    @Override
    public GroovyExpression generateGroovy(GroovyExpression parent) {
    	FunctionCallExpression result = new FunctionCallExpression(TraversalStepType.START, parent, "V");

        if(optVertexId_ != null) {
        	result.addArgument(new LiteralExpression(optVertexId_));
        }
        return result;
    }

    @Override
    public GremlinEvaluationResult evaluateOnCache(IBMGraphGraph graph) {
        if(optVertexId_ != null) {
            throw new UnsupportedOperationException();
        }
        //use special constructor that implicitly matches all vertices
        return new GremlinEvaluationResult();
    }

    @Override
    public boolean supportsEvaluation() {
        return optVertexId_ == null;
    }


    @Override
    public boolean matches(IBMGraphVertex vertex) {
        if(optVertexId_ != null) {
            throw new UnsupportedOperationException();
        }
        return true;
    }
}
