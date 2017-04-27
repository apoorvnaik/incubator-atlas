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

import java.util.Collection;

import org.apache.atlas.groovy.FunctionCallExpression;
import org.apache.atlas.groovy.LiteralExpression;
import org.apache.atlas.ibmgraph.IBMGraphGraph;
import org.apache.atlas.ibmgraph.IBMGraphVertex;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery.ComparisionOperator;

/**
 * Gremlin step that compares the value of an element property to a
 * given value
 */
public class HasStep extends AbstractHasStep {

    private Object value_;
    private AtlasGraphQuery.ComparisionOperator op_;


    public HasStep(String propertyName) {
        super(propertyName);
    }

    public HasStep(String propertyName, Object value) {
        super(propertyName);
        value_ = value;
        op_ = ComparisionOperator.EQUAL;
    }


    public HasStep(String propertyName, ComparisionOperator op, Object value) {
        super(propertyName);
        value_ = value;
        op_ = op;
    }

    public ComparisionOperator getOp() {
        return op_;
    }

    public Object getValue() {
        return value_;
    }

    @Override
    protected FunctionCallExpression translatePredicate() {
        if(op_ != null) {
        	FunctionCallExpression predicate = new FunctionCallExpression(getComparisonOperator());
            LiteralExpression translatedValue = new LiteralExpression(value_);
            predicate.addArgument(translatedValue);
            return predicate;
        }
        return null;
    }

    private String getComparisonOperator() {
        switch(op_) {
        case EQUAL:
            return "eq";
        case NOT_EQUAL:
            return "neq";
        case GREATER_THAN_EQUAL:
            return "gte";
        case LESS_THAN_EQUAL:
            return "lte";
        default:
            throw new UnsupportedOperationException("Unknown comparison operator: " + op_);
        }
    }

    /**
     * Looks up the modified vertices that meet this condition from the property index.  Note
     * that using logic like this implicitly assumes that for a given vertex, either all of
     * its properties are in the index or none of them are.  Care was taken to ensure that
     * happens.
     */
    @Override
    public GremlinEvaluationResult evaluateOnCache(IBMGraphGraph graph) {

        if( ! supportsEvaluation()) {
            throw new UnsupportedOperationException();
        }

        String propertyName = getPropertyName();
        Object propertyValue = getValue();
        return new GremlinEvaluationResult(graph.getPropertyChangeIndex().lookupMatchingVertices(propertyName, propertyValue));

    }

    @Override
    public boolean supportsEvaluation() {
        return op_ == ComparisionOperator.EQUAL;
    }

    @Override
    public boolean matches(IBMGraphVertex vertex) {

        String propertyName = getPropertyName();
        Object propertyValue = getValue();

        Collection<?> existingValues = vertex.getPropertyValues(propertyName, propertyValue.getClass());

        if(! existingValues.contains(propertyValue)) {
            return false;
        }

        return true;
    }
}
