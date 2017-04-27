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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.atlas.groovy.FunctionCallExpression;
import org.apache.atlas.groovy.LiteralExpression;
import org.apache.atlas.groovy.TraversalStepType;
import org.apache.atlas.ibmgraph.IBMGraphGraph;
import org.apache.atlas.ibmgraph.IBMGraphVertex;

/**
 * Gremlin step that compares the value of an element property to a
 * given value
 */
public class WithinStep extends AbstractHasStep {

    private Collection<? extends Object> values_;

    public WithinStep(String propertyName, Collection<? extends Object> values) {
        super(propertyName);
        values_ = values;
    }

    public Collection<? extends Object> getValues() {
        return values_;
    }

    @Override
    protected FunctionCallExpression translatePredicate() {
        FunctionCallExpression result = new FunctionCallExpression(TraversalStepType.FILTER, "within");
        Iterator<? extends Object> valueIt = values_.iterator();
        while(valueIt.hasNext()) {
            Object value = valueIt.next();
            LiteralExpression translatedValue = new LiteralExpression(value);
            result.addArgument(translatedValue);
        }
        return result;
    }

    /**
     * Looks up the modified vertices that meet this condition from the property index.  Note
     * that using logic like this implicitly assumes that for a given vertex, either all of
     * its properties are in the index or none of them are.  Care was taken to ensure that
     * happens.
     */
    @Override
    public GremlinEvaluationResult evaluateOnCache(IBMGraphGraph graph) {

        Set<IBMGraphVertex> matches = new HashSet<>();
        for(Object value : values_) {
            matches.addAll(graph.getPropertyChangeIndex().lookupMatchingVertices(getPropertyName(), value));
        }
        return new GremlinEvaluationResult(matches);

    }

    @Override
    public boolean supportsEvaluation() {
        return true;
    }

    @Override
    public boolean matches(IBMGraphVertex vertex) {

        String propertyName = getPropertyName();
        if(values_.size() == 0) {
            return false;
        }
        //check that at least one of the values in _values is present in the property
        Class clazz = values_.iterator().next().getClass();
        Collection<?> existingValues = vertex.getPropertyValues(propertyName, clazz);
        for(Object value : values_) {
            if(existingValues.contains(value)) {
                return true;
            }
        }
        return false;
    }
}
