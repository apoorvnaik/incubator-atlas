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
package org.apache.atlas.repository.graphdb.titan0;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.attribute.Text;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.Tokens;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.transform.TransformPipe;
import com.tinkerpop.pipes.util.structures.Pair;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphTraversal;
import org.apache.atlas.repository.graphdb.AtlasVertex;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

public class Titan0GraphTraversal implements AtlasGraphTraversal {
    private final Titan0Graph graph;
    private final TitanGraph backingGraph = Titan0GraphDatabase.getGraphInstance();
    private GremlinPipeline backingPipeline;
    private TitanTransaction titanTx;

    public Titan0GraphTraversal(AtlasGraph atlasGraph) {
        graph = (Titan0Graph) atlasGraph;
        titanTx = backingGraph.newTransaction();
        backingPipeline = new GremlinPipeline(backingGraph);
    }

    @Override
    public AtlasGraphTraversal V() {
        backingPipeline.V();
        return this;
    }

    @Override
    public AtlasGraphTraversal V(Object... vertexIDs) {
        backingPipeline.start(vertexIDs).V();
        return this;
    }

    @Override
    public AtlasGraphTraversal V(String key, Object value) {
        backingPipeline.V(key, value);
        return this;
    }

    @Override
    public AtlasGraphTraversal E() {
        backingPipeline.E();
        return this;
    }

    @Override
    public AtlasGraphTraversal E(Object... edgeIDs) {
        backingPipeline.start(edgeIDs).E();
        return this;
    }

    @Override
    public AtlasGraphTraversal E(String key, Object value) {
        backingPipeline.E(key, value);
        return this;
    }

    @Override
    public AtlasGraphTraversal has(String key) {
        backingPipeline.has(key);
        return this;
    }

    @Override
    public AtlasGraphTraversal has(String key, Object value) {
        backingPipeline.has(key, value);
        return this;
    }

    @Override
    public AtlasGraphTraversal has(String key, ComparisonOp comparisonOp, Object value) {
        backingPipeline.has(key, toComparisonToken(comparisonOp), value);
        return this;
    }

    @Override
    public AtlasGraphTraversal has(String key, PatternOp patternOp, Object value) {
        backingPipeline.has(key, toTP2Predicate(patternOp), value);
        return this;
    }

    @Override
    public AtlasGraphTraversal hasNot(String key) {
        backingPipeline.hasNot(key);
        return this;
    }

    @Override
    public AtlasGraphTraversal hasNot(String key, Object value) {
        backingPipeline.hasNot(key, value);
        return this;
    }

    @Override
    public AtlasGraphTraversal interval(String key, Comparable start, Comparable end) {
        backingPipeline.interval(key, start, end);
        return this;
    }

    @Override
    public AtlasGraphTraversal in(String... labels) {
        backingPipeline.in(labels);
        return this;
    }

    @Override
    public AtlasGraphTraversal inV() {
        backingPipeline.inV();
        return this;
    }

    @Override
    public AtlasGraphTraversal inE(String... labels) {
        backingPipeline.inE(labels);
        return this;
    }

    @Override
    public AtlasGraphTraversal out(String... labels) {
        backingPipeline.out(labels);
        return this;
    }

    @Override
    public AtlasGraphTraversal outV() {
        backingPipeline.outV();
        return this;
    }

    @Override
    public AtlasGraphTraversal outE(String... labels) {
        backingPipeline.outE(labels);
        return this;
    }

    @Override
    public AtlasGraphTraversal both(String... edgeLabels) {
        backingPipeline.both(edgeLabels);
        return this;
    }

    @Override
    public AtlasGraphTraversal bothE(String... edgeLabels) {
        backingPipeline.bothE(edgeLabels);
        return this;
    }

    @Override
    public AtlasGraphTraversal bothV() {
        backingPipeline.bothV();
        return this;
    }

    @Override
    public AtlasGraphTraversal property(String key) {
        backingPipeline.property(key);
        return this;
    }

    @Override
    public AtlasGraphTraversal propertyMap(String... keys) {
        backingPipeline.map(keys);
        return this;
    }

    @Override
    public AtlasGraphTraversal and(AtlasGraphTraversal[] pipelines) {
        backingPipeline.and(toGremlinPipelines(pipelines));
        return this;
    }

    @Override
    public AtlasGraphTraversal or(AtlasGraphTraversal[] pipelines) {
        backingPipeline.or(toGremlinPipelines(pipelines));
        return this;
    }

    @Override
    public AtlasGraphTraversal range(int low, int high) {
        backingPipeline.range(low, high);
        return this;
    }

    @Override
    public AtlasGraphTraversal min() {
        // todo
        return null;
    }

    @Override
    public AtlasGraphTraversal max() {
        // todo
        return null;
    }

    @Override
    public AtlasGraphTraversal sum() {
        // todo
        return null;
    }

    @Override
    public AtlasGraphTraversal cap(String namedStep) {
        backingPipeline.cap();
        return this;
    }

    @Override
    public AtlasGraphTraversal cap(int numberedStep) {
        backingPipeline.cap();
        return this;
    }

    @Override
    public AtlasGraphTraversal dedup() {
        backingPipeline.dedup();
        return this;
    }

    @Override
    public AtlasGraphTraversal filter(final Function filterFunction) {
        backingPipeline.filter(toPipeFunction(filterFunction));
        return this;
    }

    @Override
    public AtlasGraphTraversal except(final Function exceptFunction) {
        backingPipeline.filter(toPipeFunction(exceptFunction));
        return this;
    }

    @Override
    public AtlasGraphTraversal except(Collection exclude) {
        backingPipeline.except(exclude);
        return this;
    }

    @Override
    public AtlasGraphTraversal ifThenElse(Function ifFunc, Function thenFunc, Function elseFunc) {
        backingPipeline.ifThenElse(toPipeFunction(ifFunc), toPipeFunction(thenFunc), toPipeFunction(elseFunc));
        return this;
    }

    @Override
    public AtlasGraphTraversal transform(Function transformFunction) {
        backingPipeline.transform(toPipeFunction(transformFunction));
        return this;
    }

    @Override
    public AtlasGraphTraversal select(String... keys) {
        // todo Convert the keys array to a equals match function array
        return this;
    }

    @Override
    public AtlasGraphTraversal select(Function... selectFunctions) {
        backingPipeline.select(toPipeFunction(selectFunctions));
        return this;
    }

    @Override
    public AtlasGraphTraversal aggregate() {
        backingPipeline.aggregate();
        return this;
    }

    @Override
    public AtlasGraphTraversal aggregate(Collection aggregate) {
        backingPipeline.aggregate(aggregate);
        return this;
    }

    @Override
    public AtlasGraphTraversal aggregate(Collection aggregate, Function aggregateFunction) {
        backingPipeline.aggregate(aggregate, toPipeFunction(aggregateFunction));
        return this;
    }

    @Override
    public AtlasGraphTraversal aggregate(Function aggregateFunction) {
        backingPipeline.aggregate(toPipeFunction(aggregateFunction));
        return this;
    }

    @Override
    public AtlasGraphTraversal groupBy(Function keyFunction, Function valueFunction) {
        backingPipeline.groupBy(toPipeFunction(keyFunction), toPipeFunction(valueFunction));
        return this;
    }

    @Override
    public AtlasGraphTraversal groupBy(Function keyFunction, Function valueFunction, Function reduceFunction) {
        backingPipeline.groupBy(toPipeFunction(keyFunction), toPipeFunction(valueFunction), toPipeFunction(reduceFunction));
        return this;
    }

    @Override
    public AtlasGraphTraversal groupCount() {
        backingPipeline.groupCount();
        return this;
    }

    @Override
    public AtlasGraphTraversal groupCount(Function keyFunction) {
        backingPipeline.groupCount(toPipeFunction(keyFunction));
        return this;
    }

    @Override
    public AtlasGraphTraversal back(String namedStep) {
        backingPipeline.back(namedStep);
        return this;
    }

    @Override
    public AtlasGraphTraversal loop(String namedStep, Function whileFunction) {
        backingPipeline.loop(namedStep, toPipeFunction(whileFunction));
        return this;
    }

    @Override
    public AtlasGraphTraversal loop(String namedStep, Function whileFunction, Function emitFunction) {
        backingPipeline.loop(namedStep, toPipeFunction(whileFunction), toPipeFunction(emitFunction));
        return this;
    }

    @Override
    public AtlasGraphTraversal as(String name) {
        backingPipeline.as(name);
        return this;
    }

    @Override
    public AtlasGraphTraversal order() {
        backingPipeline.order();
        return this;
    }

    @Override
    public AtlasGraphTraversal order(Order order) {
        backingPipeline.order(toPipelineOrder(order));
        return this;
    }

    @Override
    public AtlasGraphTraversal orderBy(Comparator comparator) {
        backingPipeline.order(toPipeFunction(comparator));
        return this;
    }

    @Override
    public AtlasGraphTraversal path(Function[] pathFunctions) {
        backingPipeline.path(toPipeFunction(pathFunctions));
        return this;
    }

    @Override
    public AtlasGraphTraversal simplePath() {
        backingPipeline.simplePath();
        return this;
    }

    @Override
    public AtlasGraphTraversal sideEffect(Function sideEffectFunction) {
        backingPipeline.sideEffect(toPipeFunction(sideEffectFunction));
        return this;
    }

    @Override
    public AtlasGraphTraversal values() {
        // todo
        return null;
    }

    @Override
    public AtlasGraphTraversal _() {
        backingPipeline._();
        return null;
    }

    @Override
    public AtlasGraphTraversal step(Function stepFunction) {
        backingPipeline.step(toPipeFunction(stepFunction));
        return this;
    }

    @Override
    public AtlasGraphTraversal cast(Class end) {
        backingPipeline.cast(end);
        return this;
    }

    @Override
    public long count() {
        return backingPipeline.count();
    }

    @Override
    public void fill(Collection toFill) {
        backingPipeline.fill(toFill);
    }

    @Override
    public List toList() {
        // todo Maybe optimize this ?
        List backingList = backingPipeline.toList();
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
    public Set<AtlasVertex> toSet() {
        // todo Maybe optimize this ?
         return null;
    }

    public GremlinPipeline backingPipeline() {
        return backingPipeline;
    }

    private com.tinkerpop.blueprints.Predicate toTP2Predicate(PatternOp patternOp) {
        com.tinkerpop.blueprints.Predicate tp2Predicate = null;
        switch (patternOp) {
            case CONTAINS:
                tp2Predicate = Text.CONTAINS;
                break;
            case REGEX:
                tp2Predicate = Text.REGEX;
                break;
            case PREFIX:
                tp2Predicate = Text.CONTAINS_PREFIX;
                break;
            case SUFFIX:
                tp2Predicate = Text.REGEX;
                break;
        }
        return tp2Predicate;
    }

    private TransformPipe.Order toPipelineOrder(Order order) {
        TransformPipe.Order ret;
        switch (order) {
            case INCR:
                ret = TransformPipe.Order.INCR;
                break;
            case DECR:
                ret = TransformPipe.Order.DECR;
                break;
            default:
                // todo Error message
                throw new IllegalArgumentException("");
        }
        return ret;
    }

    private PipeFunction<Pair, Integer> toPipeFunction(final Comparator comparator) {
        return new PipeFunction<Pair, Integer>() {
            @Override
            public Integer compute(Pair argument) {
                return comparator.compare(argument.getA(), argument.getB());
            }
        };
    }

    private GremlinPipeline[] toGremlinPipelines(AtlasGraphTraversal[] pipelines) {
        GremlinPipeline[] gremlinPipelines = new GremlinPipeline[pipelines.length];
        for (int i = 0; i < pipelines.length; i++) {
            gremlinPipelines[i] = ((Titan0GraphTraversal) pipelines[i]).backingPipeline();
        }
        return gremlinPipelines;
    }

    private Tokens.T toComparisonToken(ComparisonOp comparisonOp) {
        switch (comparisonOp) {
            case LT:
                return Tokens.T.lt;
            case LTE:
                return Tokens.T.lte;
            case GT:
                return Tokens.T.gt;
            case GTE:
                return Tokens.T.gte;
            case EQ:
                return Tokens.T.eq;
            case NEQ:
                return Tokens.T.neq;
            case IN:
                return Tokens.T.in;
            case NOT_IN:
                return Tokens.T.notin;
            default:
                throw new IllegalArgumentException("Unknown comparison operator");
        }
    }

    // Wraps the Guava function into PipeFunction
    private PipeFunction toPipeFunction(final Function function) {
        return new PipeFunction() {
            @Override
            public Object compute(Object argument) {
                return function.apply(argument);
            }
        };
    }

    private PipeFunction[] toPipeFunction(final Function ... functions) {
        PipeFunction[] pipeFunctions = new PipeFunction[functions.length];
        for (int idx = 0; idx < functions.length; idx++) {
            pipeFunctions[idx] = toPipeFunction(functions[idx]);
        }
        return pipeFunctions;
    }
}
