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

import org.apache.atlas.groovy.AbstractFunctionExpression;
import org.apache.atlas.groovy.CastExpression;
import org.apache.atlas.groovy.FunctionCallExpression;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.GroovyGenerationContext;
import org.apache.atlas.groovy.LiteralExpression;
import org.apache.atlas.groovy.TernaryOperatorExpression;

/**
 * Expression to get a vertex or edge with the given id.
 *
 */
public class GetElementExpression extends AbstractFunctionExpression {

    public static enum ElementType {
        VERTEX,
        EDGE
    }
    private ElementType type_;
    private Object id_;

    public GetElementExpression(GroovyExpression target, ElementType type, Object id) {
        super(target);
        type_ = type;
        id_ = id;
    }

    @Override
    public void generateGroovy(GroovyGenerationContext context) {

        String getFunctionName = type_ == ElementType.VERTEX ? "vertices" : "edges";
        GroovyExpression getElementExpr = new FunctionCallExpression(getCaller(), getFunctionName, new LiteralExpression(id_));
        GroovyExpression elementListExpr = new FunctionCallExpression(getElementExpr, "toList");
        GroovyExpression isEmptyExpr = new FunctionCallExpression(elementListExpr, "isEmpty");
        LiteralExpression zero = new LiteralExpression(0);
        GroovyExpression firstElementValue = new FunctionCallExpression(elementListExpr, "get", zero);

        GroovyExpression expr = new TernaryOperatorExpression(isEmptyExpr, new LiteralExpression(null), firstElementValue);
        expr = new CastExpression(expr, type_ == ElementType.VERTEX ? "Vertex" : "Edge");
        expr.generateGroovy(context);
    }

    @Override
    public List<GroovyExpression> getChildren() {
        return Collections.singletonList(getCaller());
    }

    @Override
    public GroovyExpression copy(List<GroovyExpression> newChildren) {
        assert newChildren.size() == 1;
        return new GetElementExpression(newChildren.get(0), type_, id_);
    }

}
