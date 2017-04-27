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

package org.apache.atlas.ibmgraph.gremlin.stmt;

import java.util.ArrayList;
import java.util.List;

import org.apache.atlas.groovy.AbstractGroovyExpression;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.GroovyGenerationContext;

/**
 * Gremlin statement that represents a for loop
 */
public class ForLoopStatement extends AbstractGroovyExpression {

    private String type_;
    private String var_;
    private GroovyExpression list_;
    private GroovyExpression body_;


    public ForLoopStatement(String type, String var, GroovyExpression list, GroovyExpression body) {
        super();
        this.type_ = type;
        this.var_ = var;
        this.list_ = list;
        this.body_ = body;
    }

    public void generateGroovy(GroovyGenerationContext context) {

        context.append("for( ");
        if(type_ != null) {
            context.append(type_);
            context.append(" ");
        }
        context.append(var_);
        context.append( " in ");
        list_.generateGroovy(context);
        context.append(") ");
        body_.generateGroovy(context);
    }

    @Override
    public List<GroovyExpression> getChildren() {
        List<GroovyExpression> result = new ArrayList<>(2);
        result.add(list_);
        result.add(body_);
        return result;
    }

    @Override
    public GroovyExpression copy(List<GroovyExpression> newChildren) {
        assert newChildren.size() == 2;
        return new ForLoopStatement(type_, var_, newChildren.get(0), newChildren.get(1));
    }

}
