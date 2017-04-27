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

import java.util.Collections;
import java.util.List;

import org.apache.atlas.groovy.AbstractGroovyExpression;
import org.apache.atlas.groovy.AbstractFunctionExpression;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.GroovyGenerationContext;
import org.apache.atlas.ibmgraph.IBMGraphGraph;

/**
 * Generates a Gremlin Groovy expression that represents the
 * vertex data in the format that our code expects it be be in,
 * namely as an object with three fields: type, vertex, and outgoingEdges
 *
 * @See {@link IBMGraphGraph#convertJsonObject()}
 */
public class GetVertexDataExpression extends AbstractGroovyExpression {

    private GroovyExpression vertex_;
    private boolean includeNullCheck_ = true;

    public GetVertexDataExpression(GroovyExpression vertex) {
        vertex_ = vertex;
        includeNullCheck_ = true;
    }

    public GetVertexDataExpression(GroovyExpression vertex, boolean includeNullCheck) {
        vertex_ = vertex;
        includeNullCheck_ = includeNullCheck;
    }
    public void generateGroovy(GroovyGenerationContext context) {

        //without this check, if the vertex is null, the expression
        //to get the edges will throw a NullPointerException.  In certain
        //situations, the null check is not needed (for example if we are
        //processing a list of vertices that is the result of running a query)
        if(includeNullCheck_) {
            vertex_.generateGroovy(context);
            context.append(" == null ? null : ");
        }

        context.append("[type:'vertexdata', vertex:");
        vertex_.generateGroovy(context);
        context.append(", outgoingEdges: ");
        vertex_.generateGroovy(context);
        context.append(".edges(Direction.OUT).toList()]");
    }

    @Override
    public List<GroovyExpression> getChildren() {
        return Collections.singletonList(vertex_);
    }

    @Override
    public GroovyExpression copy(List<GroovyExpression> newChildren) {
        assert newChildren.size() == 1;
        return new GetVertexDataExpression(newChildren.get(0), includeNullCheck_);
    }
}
