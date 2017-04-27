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

package org.apache.atlas.ibmgraph.gremlin.step;

import org.apache.atlas.groovy.FunctionCallExpression;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.LiteralExpression;
import org.apache.atlas.groovy.TraversalStepType;

/**
 * Base class for different kinds of has queries
 */
public abstract class AbstractHasStep extends GremlinStep {

    private String propertyName_;

    public AbstractHasStep(String propertyName) {
        super();
        propertyName_ = propertyName;
    }


    protected abstract FunctionCallExpression translatePredicate();

    public String getPropertyName() {
        return propertyName_;
    }

    @Override
    public GroovyExpression generateGroovy(GroovyExpression parent) {
    	FunctionCallExpression result = new FunctionCallExpression(TraversalStepType.FILTER, parent, "has");
        LiteralExpression nameLiteral = new LiteralExpression(propertyName_);
        result.addArgument(nameLiteral);
        FunctionCallExpression predicate = translatePredicate();
        if(predicate != null) {
        	result.addArgument(predicate);
        }
        return result;
    }

}