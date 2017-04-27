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

package org.apache.atlas.ibmgraph.api.action;

import java.util.ArrayList;
import java.util.List;

import org.apache.atlas.groovy.ClosureExpression;
import org.apache.atlas.groovy.FunctionCallExpression;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.IdentifierExpression;
import org.apache.atlas.groovy.LiteralExpression;
import org.apache.atlas.groovy.VariableAssignmentExpression;
import org.apache.atlas.ibmgraph.api.GraphDatabaseConfiguration;
import org.apache.atlas.ibmgraph.gremlin.ActionTranslationContext;
import org.apache.atlas.ibmgraph.gremlin.expr.GraphTraversalExpression;
import org.apache.atlas.ibmgraph.gremlin.step.GremlinStep;
import org.apache.atlas.ibmgraph.gremlin.stmt.TryCatchStatement;

/**
 *
 */
public class ClearGraphAction implements IGraphAction {


    /**
     * @param graphDbVertex
     * @param propertyName
     */
    public ClearGraphAction() {

    }

    @Override
    public List<GroovyExpression> generateGremlinQuery(ActionTranslationContext context) {

        GraphTraversalExpression tr = new GraphTraversalExpression();
        GremlinStep isGraphDbVertex = GraphDatabaseConfiguration.INSTANCE.getTenantGraphStrategy().getInitialIndexedPredicateExpr();


        //add traversal def to context as a side effect
        context.getGraphTraversal();
        tr.g().V();
        if(isGraphDbVertex != null) {
            tr.addStep(isGraphDbVertex);
        }
        tr.toList();

        GroovyExpression vertices = new IdentifierExpression("vertices");
        GroovyExpression it = new IdentifierExpression("it");

        VariableAssignmentExpression getVerticesStmt = new VariableAssignmentExpression("vertices", tr.generateGroovy());

        ClosureExpression removeClosure = new ClosureExpression(new FunctionCallExpression(it, "remove"));
        FunctionCallExpression removeStmt = new FunctionCallExpression(vertices, "each", removeClosure);

        TryCatchStatement stmt = new TryCatchStatement(removeStmt);
        stmt.addCatchClause("IllegalStateException", "e"); //ignore exception if vertex does not exost
        List<GroovyExpression> result = new ArrayList<>();
        result.add(getVerticesStmt);
        result.add(stmt);
        result.add(LiteralExpression.NULL); //use null as return value of action
        return result;
    }

}
