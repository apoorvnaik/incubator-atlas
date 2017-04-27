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

package org.apache.atlas.ibmgraph.gremlin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.GroovyGenerationContext;
import org.apache.atlas.ibmgraph.gremlin.stmt.FetchTraversalStatement;

/**
 * Represents a gremlin query
 *
 */
public class GremlinQuery extends QueryElement {

    private List<GroovyExpression> stmts_ = new ArrayList<>();


      public GremlinQuery(List<GroovyExpression> steps) {
        stmts_.addAll(steps);
    }


    public GremlinQuery(GroovyExpression ... steps) {
        stmts_.add(new FetchTraversalStatement());
        stmts_.addAll(Arrays.asList(steps));
    }

    public void addStmt(GroovyExpression step) {
        stmts_.add(step);

    }

    @Override
    public void generateGroovy(GroovyGenerationContext context) {

        for(GroovyExpression step : stmts_) {
            step.generateGroovy(context);
            context.append(";");
        }

    }

    public QueryGenerationResult generateGremlin() {
        return generateGroovy(true);
    }

    public QueryGenerationResult generateGroovy(boolean useParameters) {
        GroovyGenerationContext context = new GroovyGenerationContext();
        context.setParametersAllowed(useParameters);
        generateGroovy(context);
        QueryGenerationResult result = new QueryGenerationResult();
        result.setParameters(context.getParameters());
        result.setQuery(context.getQuery());
        return result;
    }
}
