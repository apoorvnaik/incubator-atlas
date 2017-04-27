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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.atlas.groovy.AbstractGroovyExpression;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.GroovyGenerationContext;
import org.apache.atlas.groovy.StatementListExpression;

/**
 * Represents a try/catch block in Gremlin.
 */
public class TryCatchStatement extends AbstractGroovyExpression {

    private StatementListExpression body_ = new StatementListExpression();
    private List<CatchClause> catchStmts_ = new ArrayList<CatchClause>();

    private static class CatchClause extends AbstractGroovyExpression {
        private String exceptionName_;
        private String varName_;
        private StatementListExpression body_ = new StatementListExpression();

        /**
         * @param exceptionName
         * @param var
         */
        public CatchClause(String exceptionName, String var) {
            exceptionName_ = exceptionName;
            varName_ = var;
        }
        public String getExceptionName() {
            return exceptionName_;
        }

        public String getVarName() {
            return varName_;
        }

        public StatementListExpression getBody() {
            return body_;
        }
        public void addStmts(GroovyExpression... stmts) {
            for(GroovyExpression stmt : stmts) {
                body_.addStatement(stmt);
            }
        }

        public void generateGroovy(GroovyGenerationContext context) {

            context.append("catch(");
            context.append(exceptionName_);
            context.append(" ");
            context.append(varName_);
            context.append(")");
            appendStmtBlock(body_, context);
        }
        @Override
        public List<GroovyExpression> getChildren() {
            return Collections.singletonList(body_);
        }
        @Override
        public GroovyExpression copy(List<GroovyExpression> newChildren) {
            assert newChildren.size() == 0;
            assert newChildren.get(0) instanceof StatementListExpression;
            CatchClause result = new CatchClause(exceptionName_, varName_);
            result.body_ = (StatementListExpression)newChildren.get(0);
            return result;
        }
    }

    private static void appendStmtBlock(StatementListExpression exprList, GroovyGenerationContext context) {
        context.append("{");
        exprList.generateGroovy(context);
        context.append("}");
    }
    public TryCatchStatement(List<GroovyExpression> toWrap) {

        body_.addStatements(toWrap);
    }

    public TryCatchStatement(GroovyExpression... toWrap) {

        body_.addStatements(Arrays.asList(toWrap));
    }

    public void addCatchClause(String exceptionName, String var, GroovyExpression... body) {
        CatchClause catchClause = new CatchClause(exceptionName, var);
        catchClause.addStmts(body);
        catchStmts_.add(catchClause);
    }

    public void generateGroovy(GroovyGenerationContext context) {

        context.append("try ");
        appendStmtBlock(body_, context);
        for(CatchClause clause : catchStmts_) {
            clause.generateGroovy(context);
        }
    }
    @Override
    public List<GroovyExpression> getChildren() {
        List<GroovyExpression> result = new ArrayList<>(catchStmts_.size() + 1);
        result.add(body_);
        result.addAll(catchStmts_);
        return result;
    }
    @Override
    public GroovyExpression copy(List<GroovyExpression> newChildren) {
        TryCatchStatement result = new TryCatchStatement();
        result.body_ = (StatementListExpression)newChildren.get(0);
        for(GroovyExpression expr : newChildren.subList(1, newChildren.size())) {
            result.catchStmts_.add((CatchClause)expr);
        }
        return result;
    }

}
