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
import java.util.Iterator;
import java.util.List;

import org.apache.atlas.groovy.AbstractGroovyExpression;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.GroovyGenerationContext;

/**
 * Gremlin if statement
 */
public class IfStatement extends AbstractGroovyExpression {

    private List<SimpleIfStmt> conditionBlocks_ = new ArrayList<>();
    private GroovyExpression elseStmt_;

    public IfStatement(GroovyExpression condition, GroovyExpression conditionalStatement) {
        conditionBlocks_.add(new SimpleIfStmt(condition, conditionalStatement));
    }

    //for copying only
    private IfStatement() {

    }

    public void addElseIf(GroovyExpression condition, GroovyExpression conditionalStatement) {
        conditionBlocks_.add(new SimpleIfStmt(condition, conditionalStatement));
    }

    public void setElseStatement(GroovyExpression expr) {
        elseStmt_ = expr;
    }

    private static class SimpleIfStmt extends AbstractGroovyExpression {
        private GroovyExpression condition_;
        private GroovyExpression conditionalStmt_;


        /**
         * @param string
         * @param v
         */
        public SimpleIfStmt(GroovyExpression condition, GroovyExpression conditionalStmt) {
            condition_ = condition;
            conditionalStmt_ = conditionalStmt;
        }

        @Override
        public void generateGroovy(GroovyGenerationContext context) {
            context.append("if(");
            condition_.generateGroovy(context);
            context.append(") {");
            conditionalStmt_.generateGroovy(context);
            context.append("}");
        }

        @Override
        public List<GroovyExpression> getChildren() {
            List<GroovyExpression> result = new ArrayList<>(2);
            result.add(condition_);
            result.add(conditionalStmt_);
            return result;
        }

        @Override
        public GroovyExpression copy(List<GroovyExpression> newChildren) {
            assert newChildren.size() == 2;
            return new SimpleIfStmt(newChildren.get(0), newChildren.get(1));
        }
    }

    @Override
    public void generateGroovy(GroovyGenerationContext context) {
        Iterator<SimpleIfStmt> conditionIt = conditionBlocks_.iterator();
        while(conditionIt.hasNext()) {
            SimpleIfStmt ifStmt = conditionIt.next();
            ifStmt.generateGroovy(context);
            if(conditionIt.hasNext()) {
                context.append("else ");
            }
        }
        if(elseStmt_ != null) {
            context.append("else {");
            elseStmt_.generateGroovy(context);
            context.append("}");
        }
    }
    @Override
    public List<GroovyExpression> getChildren() {
        int size = expressionCount();
        List<GroovyExpression> result = new ArrayList<>(size);
        result.addAll(conditionBlocks_);
        if(elseStmt_ != null) {
            result.add(elseStmt_);
        }
        return result;
    }

    private int expressionCount() {
        int size = conditionBlocks_.size();
        if(elseStmt_ != null) {
            size++;
        }
        return size;
    }
    @Override
    public GroovyExpression copy(List<GroovyExpression> newChildren) {
        assert newChildren.size() == expressionCount();
        IfStatement result = new IfStatement();
        for(int i = 0; i < conditionBlocks_.size(); i++) {
            result.conditionBlocks_.add((SimpleIfStmt)newChildren.get(i));
        }
        if(elseStmt_ != null) {
            result.elseStmt_ = newChildren.get(newChildren.size() - 1);
        }
        return result;
    }

}
