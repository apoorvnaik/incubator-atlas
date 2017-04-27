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
import org.apache.atlas.groovy.FunctionCallExpression;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.GroovyGenerationContext;
import org.apache.atlas.groovy.IdentifierExpression;
import org.apache.atlas.groovy.LiteralExpression;

/**
 * This creates the index query gremlin script.
 *
 */
public class IndexQueryExpression extends AbstractGroovyExpression {

    private String indexName;
    private String queryString;

    public IndexQueryExpression(String indexName, String queryString) {
        this.indexName = indexName;
        this.queryString = queryString;
    }

    @Override
    public void generateGroovy(GroovyGenerationContext context) {
        IdentifierExpression graphExpr = new IdentifierExpression("graph");
        LiteralExpression indexNameExpr = new LiteralExpression(indexName);

        LiteralExpression queryexpr = new LiteralExpression(queryString);

        FunctionCallExpression fExpr = new FunctionCallExpression(graphExpr, "indexQuery", indexNameExpr, queryexpr);

        FunctionCallExpression vertexExpression = new FunctionCallExpression(fExpr, "vertices"); //will create graph.indexQuery('indexName','queryString').vertices()
        vertexExpression.generateGroovy(context);
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getQueryString() {
        return queryString;
    }

    public void setQueryString(String queryString) {
        this.queryString = queryString;
    }

    @Override
    public List<GroovyExpression> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public GroovyExpression copy(List<GroovyExpression> newChildren) {
        return new IndexQueryExpression(indexName, queryString);
    }

}
