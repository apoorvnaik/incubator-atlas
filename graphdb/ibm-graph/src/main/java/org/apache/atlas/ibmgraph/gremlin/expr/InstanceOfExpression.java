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
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.GroovyGenerationContext;

/**
 * Gremlin boolean expression that checks whether a given value is an
 * instance of a given class.
 */
public class InstanceOfExpression extends AbstractGroovyExpression {

    private GroovyExpression expr_;
    private String className_;

    public InstanceOfExpression(GroovyExpression expr, String className) {
        expr_ = expr;
        className_  =className;
    }

    @Override
    public void generateGroovy(GroovyGenerationContext context) {
        context.append("(");
        expr_.generateGroovy(context);
        context.append(" instanceof ");
        context.append(className_);
        context.append(")");
    }

    @Override
    public List<GroovyExpression> getChildren() {
        return Collections.singletonList(expr_);
    }

    @Override
    public GroovyExpression copy(List<GroovyExpression> newChildren) {
        assert newChildren.size() == 1;
        return new InstanceOfExpression(newChildren.get(0), className_);
    }

}
