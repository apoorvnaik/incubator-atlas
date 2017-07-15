/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.graphdb.titan1.query;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanMultiVertexQuery;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasMultiVertexQuery;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.titan.query.expr.OrCondition;
import org.apache.atlas.repository.graphdb.titan1.Titan1Edge;
import org.apache.atlas.repository.graphdb.titan1.Titan1Graph;
import org.apache.atlas.repository.graphdb.titan1.Titan1GraphDatabase;
import org.apache.atlas.repository.graphdb.titan1.Titan1Vertex;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Titan1AtlasMultiVertexQuery implements AtlasMultiVertexQuery<Titan1Vertex, Titan1Edge, Titan1AtlasMultiVertexQuery> {

    private final StandardTitanGraph standardTitanGraph;
    private final Titan1Graph titan1Graph;
    private final OrCondition queryCondition = new OrCondition();
    private final boolean isChildQuery;
    private TitanMultiVertexQuery multiVertexQuery;

    public Titan1AtlasMultiVertexQuery(Titan1Graph graph) {
        titan1Graph         = graph;
        standardTitanGraph  = (StandardTitanGraph) Titan1GraphDatabase.getGraphInstance();
        isChildQuery        = false;
    }

    @VisibleForTesting
    public Titan1AtlasMultiVertexQuery(Titan1Graph graph, StandardTitanGraph standardTitanGraph, boolean isChildQuery) {
        titan1Graph         = graph;
        this.standardTitanGraph = standardTitanGraph;
        this.isChildQuery       = isChildQuery;
    }

    @Override
    public Titan1AtlasMultiVertexQuery addVertex(AtlasVertex<Titan1Vertex, Titan1Edge> vertex) {
        TitanVertex wrappedElement = vertex.getWrappedElement();
        if (multiVertexQuery == null) {
            multiVertexQuery = standardTitanGraph.multiQuery(wrappedElement);
        } else {
            multiVertexQuery.addVertex(wrappedElement);
        }
        return this;
    }

    @Override
    public Titan1AtlasMultiVertexQuery addAllVertice(Collection<AtlasVertex<Titan1Vertex, Titan1Edge>> vertices) {
        Collection<TitanVertex> titanVertices = new ArrayList<>();
        for (AtlasVertex<Titan1Vertex, Titan1Edge> vertex : vertices) {
            titanVertices.add((TitanVertex) vertex.getWrappedElement());
        }
        if (multiVertexQuery == null) {
            multiVertexQuery = standardTitanGraph.multiQuery(titanVertices);
        } else {
            multiVertexQuery.addAllVertices(titanVertices);
        }
        return this;
    }

    @Override
    public Titan1AtlasMultiVertexQuery labels(String... labels) {
        Preconditions.checkNotNull(multiVertexQuery);
        multiVertexQuery.labels(labels);
        return this;
    }

    @Override
    public Titan1AtlasMultiVertexQuery keys(String... keys) {
        Preconditions.checkNotNull(multiVertexQuery);
        multiVertexQuery.keys(keys);
        return this;
    }

    @Override
    public Titan1AtlasMultiVertexQuery direction(AtlasEdgeDirection direction) {
        Preconditions.checkNotNull(multiVertexQuery);
        multiVertexQuery.direction(toTitanDirection(direction));
        return this;
    }

    @Override
    public Titan1AtlasMultiVertexQuery has(String key) {
        Preconditions.checkNotNull(multiVertexQuery);
        multiVertexQuery.has(key);
        return this;
    }

    @Override
    public Titan1AtlasMultiVertexQuery hasNot(String key) {
        Preconditions.checkNotNull(multiVertexQuery);
        multiVertexQuery.hasNot(key);
        return this;
    }

    @Override
    public Titan1AtlasMultiVertexQuery has(String key, Object value) {
        Preconditions.checkNotNull(multiVertexQuery);
        multiVertexQuery.has(key, value);
        return this;
    }

    @Override
    public Titan1AtlasMultiVertexQuery hasNot(String key, Object value) {
        Preconditions.checkNotNull(multiVertexQuery);
        multiVertexQuery.hasNot(key, value);
        return this;
    }

    @Override
    public Titan1AtlasMultiVertexQuery has(String key, AtlasGraphQuery.QueryOperator operator, Object value) {
        Preconditions.checkNotNull(multiVertexQuery);
        multiVertexQuery.has(key, toTitanPredicate(operator), value);
        return this;
    }

    @Override
    public <T extends Comparable<?>> Titan1AtlasMultiVertexQuery interval(String key, T start, T end) {
        Preconditions.checkNotNull(multiVertexQuery);
        multiVertexQuery.interval(key, start, end);
        return this;
    }

    @Override
    public Titan1AtlasMultiVertexQuery or(Titan1AtlasMultiVertexQuery ... multiVertexQueries) {
        return or(Arrays.asList(multiVertexQueries));
    }

    @Override
    public Titan1AtlasMultiVertexQuery or(List<Titan1AtlasMultiVertexQuery> childQueries) {
        //Construct an overall OrCondition by combining all of the children for
        //the OrConditions in all of the childQueries that we passed in.  Then, "and" the current
        //query condition with this overall OrCondition.

        OrCondition overallChildQuery = new OrCondition(false);

        for(Titan1AtlasMultiVertexQuery childQuery : childQueries) {
            if (!childQuery.isChildQuery()) {
                throw new IllegalArgumentException(childQuery + " is not a child query");
            }
            overallChildQuery.orWith(childQuery.getOrCondition());
        }

        queryCondition.andWith(overallChildQuery);
        return this;
    }

    @Override
    public Titan1AtlasMultiVertexQuery createChildQuery() {
        return new Titan1AtlasMultiVertexQuery(titan1Graph, standardTitanGraph, true);
    }

    @Override
    public boolean isChildQuery() {
        return isChildQuery;
    }

    @Override
    public Titan1AtlasMultiVertexQuery addConditionsFrom(Titan1AtlasMultiVertexQuery otherQuery) {
        queryCondition.andWith(otherQuery.queryCondition);
        return this;
    }

    @Override
    public Titan1AtlasMultiVertexQuery limit(int limit) {
        Preconditions.checkNotNull(multiVertexQuery);
        multiVertexQuery.limit(limit);
        return this;
    }

    @Override
    public Map<Titan1Vertex, Iterable<AtlasPropertyKey>> properties() {
        Preconditions.checkNotNull(multiVertexQuery);
        Map<Titan1Vertex, Iterable<AtlasPropertyKey>> ret = new HashMap<>();
        Map<TitanVertex, Iterable<TitanProperty>> properties = multiVertexQuery.properties();

        // todo

        return ret;
    }

    @Override
    public Map<Titan1Vertex, Iterable<Titan1Edge>> edges() {
        Preconditions.checkNotNull(multiVertexQuery);
        Map<Titan1Vertex, Iterable<Titan1Edge>> ret = new HashMap<>();
        Map<TitanVertex, Iterable<TitanEdge>> properties = multiVertexQuery.edges();

        // todo

        return ret;
    }

    @Override
    public Map<Titan1Vertex, Iterable<Titan1Vertex>> vertices() {
        Preconditions.checkNotNull(multiVertexQuery);
        Map<Titan1Vertex, Iterable<Titan1Vertex>> ret = new HashMap<>();
        Map<TitanVertex, Iterable<TitanVertex>> properties = multiVertexQuery.vertices();

        // todo

        return ret;
    }

    @Override
    public Iterable<Titan1Edge> edges(boolean flatten) {
        Preconditions.checkNotNull(multiVertexQuery);
        Collection<Titan1Edge> flattenedEdges = new ArrayList<>();
        Map<TitanVertex, Iterable<TitanEdge>> properties = multiVertexQuery.edges();

        // todo

        return flattenedEdges;
    }

    @Override
    public Iterable<Titan1Vertex> vertices(boolean flatten) {
        Preconditions.checkNotNull(multiVertexQuery);
        Collection<Titan1Vertex> flattenedVertices = new ArrayList<>();
        Map<TitanVertex, Iterable<TitanEdge>> properties = multiVertexQuery.edges();

        // todo

        return flattenedVertices;
    }

    @Override
    public Map<Titan1Vertex, Iterable<Titan1Edge>> edges(int limit) {
        Preconditions.checkNotNull(multiVertexQuery);
        Map<Titan1Vertex, Iterable<Titan1Edge>> ret = new HashMap<>();
        Map<TitanVertex, Iterable<TitanProperty>> properties = multiVertexQuery.properties();

        // todo

        return ret;
    }

    @Override
    public Map<Titan1Vertex, Iterable<Titan1Edge>> edges(int offset, int limit) {
        Preconditions.checkNotNull(multiVertexQuery);
        Map<Titan1Vertex, Iterable<Titan1Edge>> ret = new HashMap<>();
        Map<TitanVertex, Iterable<TitanProperty>> properties = multiVertexQuery.properties();

        // todo

        return ret;
    }

    @Override
    public Map<Titan1Vertex, Iterable<Titan1Vertex>> vertices(int limit) {
        Preconditions.checkNotNull(multiVertexQuery);
        Map<Titan1Vertex, Iterable<Titan1Vertex>> ret = new HashMap<>();
        Map<TitanVertex, Iterable<TitanProperty>> properties = multiVertexQuery.properties();

        // todo

        return ret;
    }

    @Override
    public Map<Titan1Vertex, Iterable<Titan1Vertex>> vertices(int offset, int limit) {
        Preconditions.checkNotNull(multiVertexQuery);
        Map<Titan1Vertex, Iterable<Titan1Vertex>> ret = new HashMap<>();

        // todo

        return ret;
    }

    @Override
    public Iterable<Titan1Edge> edges(int limit, boolean flatten) {
        Preconditions.checkNotNull(multiVertexQuery);
        Collection<Titan1Edge> flattenedEdges = new ArrayList<>();
        Map<TitanVertex, Iterable<TitanEdge>> properties = multiVertexQuery.edges();

        // todo

        return flattenedEdges;
    }

    @Override
    public Iterable<Titan1Edge> edges(int offset, int limit, boolean flatten) {
        Preconditions.checkNotNull(multiVertexQuery);
        Collection<Titan1Edge> flattenedEdges = new ArrayList<>();
        Map<TitanVertex, Iterable<TitanEdge>> properties = multiVertexQuery.edges();

        // todo

        return flattenedEdges;
    }

    @Override
    public Iterable<Titan1Vertex> vertices(int limit, boolean flatten) {
        Preconditions.checkNotNull(multiVertexQuery);
        multiVertexQuery.limit(limit);
        Collection<Titan1Vertex> flattenedVertices = new ArrayList<>();

        // todo

        return flattenedVertices;
    }

    @Override
    public Iterable<Titan1Vertex> vertices(int offset, int limit, boolean flatten) {
        Preconditions.checkNotNull(multiVertexQuery);
        Collection<Titan1Vertex> flattenedVertices = new ArrayList<>();

        // todo

        return flattenedVertices;
    }

    private TitanPredicate toTitanPredicate(AtlasGraphQuery.QueryOperator operator) {
        return null;
    }

    private Direction toTitanDirection(AtlasEdgeDirection direction) {
        switch (direction) {
            case IN:
                return Direction.IN;
            case OUT:
                return Direction.OUT;
            case BOTH:
                return Direction.BOTH;
            default:
                throw new IllegalArgumentException("Invalid direction");
        }
    }

    private OrCondition getOrCondition() {
        return queryCondition;
    }
}
