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
import java.util.List;

import org.apache.atlas.groovy.AbstractFunctionExpression;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.GroovyGenerationContext;

/**
 * A collect expression is Groovy method that can be called on a list
 * to produce another list whose contents are derived from the original
 * list by calling a function.  The function is provided as an anonymous
 * function where the value of the list element being converted is
 * stored in the variable "it"
 */
public class CollectExpression extends AbstractFunctionExpression {

    private GroovyExpression conversionExpr_;

    public CollectExpression(GroovyExpression listExpr, GroovyExpression conversionExpr) {
        super(listExpr);
        conversionExpr_ = conversionExpr;
    }

    @Override
    public void generateGroovy(GroovyGenerationContext context) {
        getCaller().generateGroovy(context);
        context.append(".collect{");
        conversionExpr_.generateGroovy(context);
        context.append("}");
    }

    @Override
    public List<GroovyExpression> getChildren() {
        List<GroovyExpression> children = new ArrayList<>(2);
        children.add(getCaller());
        children.add(conversionExpr_);
        return children;
    }

    @Override
    public GroovyExpression copy(List<GroovyExpression> newChildren) {
        assert newChildren.size() == 2;
        return new CollectExpression(newChildren.get(0), newChildren.get(1));
    }
}
