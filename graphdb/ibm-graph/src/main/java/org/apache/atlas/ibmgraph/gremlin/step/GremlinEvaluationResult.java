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

import java.util.Set;

import org.apache.atlas.ibmgraph.IBMGraphVertex;

import com.google.common.collect.Sets;

/**
 * The result of executing an ExecutableGremlinQuery on the uncommitted changes
 * from the current transaction.
 */
public class GremlinEvaluationResult {

    //special flag that indicates that the result currently matches all
    //vertices in the database.  We don's support this and don't want to
    //get all vertices just to match the initial condition in the query.  However,
    //we need the intersection behavior to be correct so that queries return
    //the right result.
    private boolean isAllVertices_;

    //the vertices in the result, or null if all vertices are being matched
    private Set<IBMGraphVertex> matchingVertices_ = null;

    public GremlinEvaluationResult() {
        isAllVertices_ = true;
    }
    public GremlinEvaluationResult(Set<IBMGraphVertex> matchingVertices) {
        matchingVertices_ = matchingVertices;
    }

    /**
     * Whether this result matches all vertices in the graph.
     *
     * @return
     */
    public boolean isAllVertices() {
        return isAllVertices_;
    }

    /**
     * Gets the vertices from the result.
     *
     * @return
     */
    public Set<IBMGraphVertex> getMatchingVertices() {
        if(matchingVertices_ == null) {
            //getting all vertices from the graph is not currently supported
            //by this mechanism
            throw new UnsupportedOperationException();
        }
        return matchingVertices_;
    }

    /**
     * Creates a result with only contains vertices that are present both in
     * this result and the other evaluation result.
     *
     * @param other
     * @return
     */
    public GremlinEvaluationResult intersectWith(GremlinEvaluationResult other) {

        if(isAllVertices_) {
            return other;
        }
        if(other.isAllVertices()) {
            return this;
        }
        Set<IBMGraphVertex> result = Sets.intersection(
                matchingVertices_,
                other.getMatchingVertices());
        return new GremlinEvaluationResult(result);
    }
}
