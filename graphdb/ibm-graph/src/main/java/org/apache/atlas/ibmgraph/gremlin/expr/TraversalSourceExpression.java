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

/**
 *
 */
public class TraversalSourceExpression extends AbstractGroovyExpression {

    private String partitionKey;
    private GroovyExpression tenantId;


    public TraversalSourceExpression(GroovyExpression tenantId, String partitionKey) {
        this.tenantId = tenantId;
        this.partitionKey = partitionKey;
    }

    @Override
    public void generateGroovy(GroovyGenerationContext context) {
        GroovyExpression graph = new IdentifierExpression("graph");
        GroovyExpression strategy = new CreatePartitionExpression(tenantId, partitionKey);
        GroovyExpression result = new IdentifierExpression("GraphTraversalSource");
        result = new FunctionCallExpression(result, "build");
        result = new FunctionCallExpression(result, "with", strategy);
        result = new FunctionCallExpression(result, "create", graph);
        result.generateGroovy(context);
    }

    @Override
    public List<GroovyExpression> getChildren() {
        return Collections.singletonList(tenantId);
    }

    @Override
    public GroovyExpression copy(List<GroovyExpression> newChildren) {
        assert newChildren.size() == 1;
        return new TraversalSourceExpression(newChildren.get(0), partitionKey);
    }

}
