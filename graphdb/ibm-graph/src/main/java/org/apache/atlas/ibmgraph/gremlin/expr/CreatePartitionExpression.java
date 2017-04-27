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
 * Gremlin boolean expression that compares two expressions using
 * a comparison operator.
 */
public class CreatePartitionExpression extends AbstractGroovyExpression {

    private GroovyExpression tenantId_;
    private String partitionKey;

    /**
     *
     * @param expr1 first expression
     * @param op binary operator to use.  Must be one of '=','!=','>','>=','<','<='.
     * @param expr2 second expression
     */
    public CreatePartitionExpression(GroovyExpression tenantId, String partitionKey) {
        tenantId_ = tenantId;
        this.partitionKey = partitionKey;
    }

    @Override
    public void generateGroovy(GroovyGenerationContext context) {

        GroovyExpression expr = new IdentifierExpression("PartitionStrategy");
        expr = new FunctionCallExpression(expr,"build");
        expr = new FunctionCallExpression(expr, "partitionKey", new LiteralExpression(partitionKey));
        expr = new FunctionCallExpression(expr, "addReadPartition", tenantId_);
        expr = new FunctionCallExpression(expr, "writePartition", tenantId_);
        expr = new FunctionCallExpression(expr, "create");
        expr.generateGroovy(context);
    }

    @Override
    public List<GroovyExpression> getChildren() {
        return Collections.singletonList(tenantId_);
    }

    @Override
    public GroovyExpression copy(List<GroovyExpression> newChildren) {
        assert newChildren.size() == 1;
        CreatePartitionExpression result = new CreatePartitionExpression(newChildren.get(0), partitionKey);
        return result;
    }

}
