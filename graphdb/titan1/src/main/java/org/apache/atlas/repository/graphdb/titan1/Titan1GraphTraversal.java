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
package org.apache.atlas.repository.graphdb.titan1;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.regex.Pattern;

public class Titan1GraphTraversal implements AtlasGraphTraversal {
    private static TitanGraph titanGraph = Titan1GraphDatabase.getGraphInstance();

    private final GraphTraversalSource traversalSource;
    private final Titan1Graph graph;

    private GraphTraversal traversal;
    private TitanTransaction titanTx;

    public Titan1GraphTraversal(AtlasGraph atlasGraph) {
        graph           = (Titan1Graph) atlasGraph;
        traversalSource = titanGraph.traversal();
        titanTx = titanGraph.newTransaction();
    }

    @Override
    public AtlasGraphTraversal V() {
        traversal = traversalSource.V(null);
        return this;
    }

    @Override
    public AtlasGraphTraversal V(Object... vertexIDs) {
        traversal = traversalSource.V(vertexIDs);
        return this;
    }

    @Override
    public AtlasGraphTraversal V(String key, Object value) {
        V();
        traversal.has(key, value);
        return this;
    }

    @Override
    public AtlasGraphTraversal E() {
        traversal = traversalSource.E(null);
        return this;
    }

    @Override
    public AtlasGraphTraversal E(Object... edgeIDs) {
        traversal = traversalSource.E(edgeIDs);
        return this;
    }

    @Override
    public AtlasGraphTraversal E(String key, Object value) {
        E();
        traversal.has(T.key, key).has(T.value, value);
        return this;
    }

    @Override
    public AtlasGraphTraversal has(String key) {
        traversal.has(key);
        return this;
    }

    @Override
    public AtlasGraphTraversal has(String key, Object value) {
        traversal.has(key, value);
        return this;
    }

    @Override
    public AtlasGraphTraversal has(String key, ComparisonOp comparisonOp, Object value) {
        traversal.has(key).has(T.value, toTitanPredicate(comparisonOp, value));
        return this;
    }

    @Override
    public AtlasGraphTraversal has(String key, PatternOp patternOp, Object value) {
        // Ignore the patternOp as there's no native support in TP3
        traversal.has(key, toTP3Predicate(value));
        return this;
    }

    @Override
    public AtlasGraphTraversal hasNot(String key) {
        traversal.hasNot(key);
        return this;
    }

    @Override
    public AtlasGraphTraversal hasNot(String key, Object value) {
        traversal.has(T.key, P.neq(key)).has(T.value, P.neq(value));
        return this;
    }

    @Override
    public AtlasGraphTraversal interval(String key, Comparable start, Comparable end) {
        traversal.has(T.key, key).has(T.value, P.between(start, end));
        return this;
    }

    @Override
    public AtlasGraphTraversal in(String... labels) {
        traversal.in(labels);
        return this;
    }

    @Override
    public AtlasGraphTraversal inV() {
        traversal.inV();
        return this;
    }

    @Override
    public AtlasGraphTraversal inE(String... labels) {
        traversal.inE(labels);
        return this;
    }

    @Override
    public AtlasGraphTraversal out(String... labels) {
        traversal.out(labels);
        return this;
    }

    @Override
    public AtlasGraphTraversal outV() {
        traversal.outV();
        return this;
    }

    @Override
    public AtlasGraphTraversal outE(String... labels) {
        traversal.outE(labels);
        return this;
    }

    @Override
    public AtlasGraphTraversal both(String... edgeLabels) {
        traversal.both(edgeLabels);
        return this;
    }

    @Override
    public AtlasGraphTraversal bothE(String... edgeLabels) {
        traversal.bothE(edgeLabels);
        return this;
    }

    @Override
    public AtlasGraphTraversal bothV() {
        traversal.bothV();
        return this;
    }

    @Override
    public AtlasGraphTraversal property(String key) {
        traversal.properties(key);
        return this;
    }

    @Override
    public AtlasGraphTraversal propertyMap(String... keys) {
        traversal.propertyMap(keys);
        return this;
    }

    @Override
    public AtlasGraphTraversal and(AtlasGraphTraversal[] pipelines) {
        traversal.and(toTraversal(pipelines));
        return this;
    }

    @Override
    public AtlasGraphTraversal or(AtlasGraphTraversal[] pipelines) {
        traversal.or(toTraversal(pipelines));
        return this;
    }

    @Override
    public AtlasGraphTraversal range(int low, int high) {
        traversal.range(low, high);
        return this;
    }

    @Override
    public AtlasGraphTraversal min() {
        traversal.min();
        return this;
    }

    @Override
    public AtlasGraphTraversal max() {
        traversal.max();
        return this;
    }

    @Override
    public AtlasGraphTraversal sum() {
        traversal.sum();
        return this;
    }

    @Override
    public AtlasGraphTraversal cap(String namedStep) {
        traversal.cap(namedStep);
        return this;
    }

    @Override
    public AtlasGraphTraversal cap(int numberedStep) {
        // todo
        return null;
    }

    @Override
    public AtlasGraphTraversal dedup() {
        traversal.dedup();
        return this;
    }

    @Override
    public AtlasGraphTraversal filter(Function filterFunction) {
        traversal.filter(toTraverserPredicate(filterFunction));
        return this;
    }

    @Override
    public AtlasGraphTraversal except(Function exceptFunction) {
        traversal.filter(toTraverserPredicate(exceptFunction));
        return this;
    }

    @Override
    public AtlasGraphTraversal except(Collection exclude) {
        traversal.filter(P.without(exclude));
        return this;
    }

    @Override
    public AtlasGraphTraversal ifThenElse(Function ifFunc, Function thenFunc, Function elseFunc) {
        // todo Do we need this ?
        return this;
    }

    @Override
    public AtlasGraphTraversal transform(final Function transformFunction) {
        traversal.map(new java.util.function.Function() {
            @Override
            public Object apply(Object o) {
                return transformFunction.apply(o);
            }
        });
        return this;
    }

    @Override
    public AtlasGraphTraversal select(String... keys) {
        traversal.select(keys[0], keys[1], Arrays.copyOfRange(keys, 2, keys.length));
        return this;
    }

    @Override
    public AtlasGraphTraversal select(Function... selectFunction) {
        // todo Map the select functions
        return this;
    }

    @Override
    public AtlasGraphTraversal aggregate() {
        // todo Check if null works
        traversal.aggregate(null);
        return this;
    }

    @Override
    public AtlasGraphTraversal aggregate(Collection aggregate) {
        aggregate().fill(aggregate);
        return this;
    }

    @Override
    public AtlasGraphTraversal aggregate(Collection aggregate, Function aggregateFunction) {
        List preAggregateFunction = aggregate().toList();
        // todo Complete this aggregation
        return this;
    }

    @Override
    public AtlasGraphTraversal aggregate(Function aggregateFunction) {
        // todo
        return this;
    }

    @Override
    public AtlasGraphTraversal groupBy(final Function keyFunction, final Function valueFunction) {
        // todo test this out
        traversal.group().by(new java.util.function.Function() {
            @Override
            public Object apply(Object o) {
                return keyFunction.apply(o);
            }
        }).by(new java.util.function.Function() {
            @Override
            public Object apply(Object o) {
                return valueFunction.apply(o);
            }
        });
        return this;
    }

    @Override
    public AtlasGraphTraversal groupBy(Function keyFunction, Function valueFunction, Function reduceFunction) {
        // todo
        throw new UnsupportedOperationException("Reduction is not supported");
    }

    @Override
    public AtlasGraphTraversal groupCount() {
        traversal.groupCount();
        return this;
    }

    @Override
    public AtlasGraphTraversal groupCount(Function keyFunction) {
        // todo
        return this;
    }

    @Override
    public AtlasGraphTraversal back(String namedStep) {
        // todo
        return this;
    }

    @Override
    public AtlasGraphTraversal loop(String namedStep, Function whileFunction) {
        // todo
        return this;
    }

    @Override
    public AtlasGraphTraversal loop(String namedStep, Function whileFunction, Function emitFunction) {
        // todo
        return this;
    }

    @Override
    public AtlasGraphTraversal as(String name) {
        traversal.as(name);
        return this;
    }

    @Override
    public AtlasGraphTraversal order() {
        traversal.order();
        return this;
    }

    @Override
    public AtlasGraphTraversal order(Order order) {
        traversal.order();
        return this;
    }

    @Override
    public AtlasGraphTraversal orderBy(Comparator comparator) {
        // todo
        return this;
    }

    @Override
    public AtlasGraphTraversal path(Function[] pathFunctions) {
        traversal.path();
        return this;
    }

    @Override
    public AtlasGraphTraversal simplePath() {
        traversal.simplePath();
        return this;
    }

    @Override
    public AtlasGraphTraversal sideEffect(Function sideEffectFunction) {
        // todo
        return this;
    }

    @Override
    public AtlasGraphTraversal values() {
        // todo
        traversal.mapValues();
        return this;
    }

    @Override
    public AtlasGraphTraversal _() {
        // todo
        return this;
    }

    @Override
    public AtlasGraphTraversal step(Function stepFunction) {
        traversal.emit(toTraverserPredicate(stepFunction));
        return this;
    }

    @Override
    public AtlasGraphTraversal cast(Class end) {

        return null;
    }

    @Override
    public long count() {
        // todo
        return 0;
    }

    @Override
    public void fill(Collection toFill) {
        traversal.fill(toFill);
    }

    @Override
    public List toList() {
        // todo Maybe optimize this ?
        List backingList = traversal.toList();
        Object first = backingList.get(0);
        List ret;
        if (first instanceof Vertex) {
            ret = Lists.newArrayList(graph.wrapVertices(backingList));
        } else if (first instanceof Edge) {
            ret = Lists.newArrayList(graph.wrapEdges(backingList));
        } else {
            ret = backingList;
        }
        return ret;
    }

    @Override
    public Set toSet() {
        return traversal.toSet();
    }

    public Traversal backingTraversal() {
        return traversal;
    }

    private P<?> toTP3Predicate(Object value) {
        // Since this is only being used during pattern matching, we've to treat the value as a REGEX pattern
        // and test the pipeline values against that
        final Pattern pattern = Pattern.compile((String) value);
        return new P<>(new BiPredicate<String, String>() {
            @Override
            public boolean test(String testValue, String originalValue) {
                return pattern.matcher(testValue).matches();
            }
        }, (String) value);
    }

    private void startTx() {
        titanTx = Titan1GraphDatabase.getGraphInstance().newTransaction();
    }

    private P<?> toTitanPredicate(ComparisonOp comparisonOp, Object value) {
        P<?> titanPredicate;
        switch (comparisonOp) {
            case LT:
                titanPredicate = P.lt(value);
                break;
            case LTE:
                titanPredicate = P.lte(value);
                break;
            case GT:
                titanPredicate = P.gt(value);
                break;
            case GTE:
                titanPredicate = P.gte(value);
                break;
            case EQ:
                titanPredicate = P.eq(value);
                break;
            case NEQ:
                titanPredicate = P.neq(value);
                break;
            case IN:
                titanPredicate = P.within(value);
                break;
            case NOT_IN:
                titanPredicate = P.without(value);
                break;
            default:
                throw new IllegalArgumentException("Unsupported predicate " + comparisonOp);
        }
        return titanPredicate;
    }

    private Traversal<?, ?>[] toTraversal(AtlasGraphTraversal[] pipelines) {
        Traversal[] traversals = new Traversal[pipelines.length];
        for (int idx = 0; idx < pipelines.length; idx++) {
            AtlasGraphTraversal pipeline = pipelines[idx];
            traversals[idx] = ((Titan1GraphTraversal)pipeline).backingTraversal();
        }
        return traversals;
    }

    // todo
    private <E> java.util.function.Predicate<Traverser<E>> toTraverserPredicate(Function function) {
        return null;
    }
}
