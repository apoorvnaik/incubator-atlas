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

package org.apache.atlas.ibmgraph;

import java.util.Collection;
import java.util.HashSet;

import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.AtlasVertexQuery;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
/**
 * This class runs the queries at vertices end point.
 * If no direction is specified then it will default to both.
 *
 */
public class IBMGraphVertexQuery implements AtlasVertexQuery<IBMGraphVertex, IBMGraphEdge> {
    private IBMGraphVertex vertex_;
    private AtlasEdgeDirection direction = AtlasEdgeDirection.BOTH;
    public IBMGraphVertexQuery(IBMGraphVertex vertex) {
        super();
        vertex_ = vertex;

    }

    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasVertexQuery#direction(org.apache.atlas.repository.graphdb.AtlasEdgeDirection)
     */
    @Override
    public AtlasVertexQuery<IBMGraphVertex, IBMGraphEdge> direction(AtlasEdgeDirection queryDirection) {
        this.direction = queryDirection;
        return this;
    }

    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasVertexQuery#vertices()
     */
    @Override
    public Iterable<AtlasVertex<IBMGraphVertex, IBMGraphEdge>> vertices() {

        //determine the matching vertices by traversing the matching edges.  This allows
        //us to distinguish cases where there are multiple edges between edges and,
        //for example, only one was removed (vs all of them being removed).
        Iterable<AtlasEdge<IBMGraphVertex, IBMGraphEdge>> edges = edges();

        Collection<AtlasVertex<IBMGraphVertex, IBMGraphEdge>> result = new HashSet<>();
        for(AtlasEdge<IBMGraphVertex, IBMGraphEdge> edge : edges) {
            result.add(edge.getInVertex());
            result.add(edge.getOutVertex());
        }
        result.remove(vertex_);
        return result;
    }


    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasVertexQuery#edges()
     */
    @Override
    public Iterable<AtlasEdge<IBMGraphVertex, IBMGraphEdge>> edges() {

        return vertex_.getEdges(direction, null);
    }

    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasVertexQuery#count()
     */
    @Override
    public long count() {
        return IteratorUtils.count(edges());
    }

}