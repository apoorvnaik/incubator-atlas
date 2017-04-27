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

import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.ibmgraph.IBMGraphGraph;
import org.apache.atlas.ibmgraph.IBMGraphVertex;
import org.apache.atlas.ibmgraph.gremlin.expr.EvaluableGremlinExpression;

/**
 * Base class for gremlin steps.  These are used for generating
 * graph traversals (aka gremlin queries)
 */
public abstract class GremlinStep implements EvaluableGremlinExpression {

	public abstract GroovyExpression generateGroovy(GroovyExpression parent);

    @Override
    public GremlinEvaluationResult evaluateOnCache(IBMGraphGraph graph) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsEvaluation() {
        return false;
    }

    @Override
    public boolean matches(IBMGraphVertex vertex) {
        return false;
    }



}
