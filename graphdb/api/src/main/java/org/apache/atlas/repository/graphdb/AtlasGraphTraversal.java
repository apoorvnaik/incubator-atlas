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
package org.apache.atlas.repository.graphdb;

import com.google.common.base.Function;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface AtlasGraphTraversal<S, E> {
    enum ComparisonOp {
        LT,
        LTE,
        GT,
        GTE,
        EQ,
        NEQ,
        IN,
        NOT_IN,
    }

    enum PatternOp {
        CONTAINS,
        REGEX,
        PREFIX,
        SUFFIX
    }

    enum Order { INCR, DECR }

    interface Predicate {

        /**
         * If the underlying graph does not support the push-down predicate, then an in-memory evaluation can be done.
         *
         * @param first  the left hand side of the predicate
         * @param second the right hand side of the predicate
         * @return the truth of the predicate given the two arguments
         */
        boolean evaluate(final Object first, final Object second);
    }

    /*
        Traversal start expressions
     */
    AtlasGraphTraversal<S, ? extends AtlasVertex> V();
    AtlasGraphTraversal<S, ? extends AtlasVertex> V(final Object ... vertexIDs);
    AtlasGraphTraversal<S, ? extends AtlasVertex> V(final String key, final Object value);

    AtlasGraphTraversal<S, ? extends AtlasEdge> E();
    AtlasGraphTraversal<S, ? extends AtlasEdge> E(final Object ... edgeIDs);
    AtlasGraphTraversal<S, ? extends AtlasEdge> E(final String key, final Object value);

    /*
        Filter expressions
     */
    AtlasGraphTraversal<S, ? extends AtlasElement> has(final String key);
    AtlasGraphTraversal<S, ? extends AtlasElement> has(final String key, final Object value);
    AtlasGraphTraversal<S, ? extends AtlasElement> has(final String key, final ComparisonOp comparisonOp, final Object value);
    AtlasGraphTraversal<S, ? extends AtlasElement> has(final String key, final PatternOp patternOp, final Object value);
    AtlasGraphTraversal<S, ? extends AtlasElement> hasNot(final String key);
    AtlasGraphTraversal<S, ? extends AtlasElement> hasNot(final String key, final Object value);
    AtlasGraphTraversal<S, ? extends AtlasElement> interval(final String key, final Comparable start, final Comparable end);

    /*
        Adjacent element (vertex/edge) expressions
     */
    AtlasGraphTraversal<S, ? extends AtlasVertex> in(final String ... labels);
    AtlasGraphTraversal<S, ? extends AtlasVertex> inV();
    AtlasGraphTraversal<S, ? extends AtlasEdge> inE(final String ... labels);

    AtlasGraphTraversal<S, ? extends AtlasVertex> out(final String ... labels);
    AtlasGraphTraversal<S, ? extends AtlasVertex> outV();
    AtlasGraphTraversal<S, ? extends AtlasEdge> outE(final String ... labels);

    AtlasGraphTraversal<S, ? extends AtlasVertex> both(final String ... edgeLabels);
    AtlasGraphTraversal<S, ? extends AtlasEdge> bothE(final String ... edgeLabels);
    AtlasGraphTraversal<S, ? extends AtlasVertex> bothV();

    AtlasGraphTraversal<S, Object> property(final String key);
    AtlasGraphTraversal<S, Map<String, Object>> propertyMap(final String ... keys);

    /*
        Multi-query operations
     */
    AtlasGraphTraversal<S, ? extends AtlasEdge> and(final AtlasGraphTraversal<S, E>... pipelines);
    AtlasGraphTraversal<S, ? extends AtlasEdge> or(final AtlasGraphTraversal<S, E>... pipelines);

    AtlasGraphTraversal<S, ? extends AtlasEdge> range(final int low, final int high);
    AtlasGraphTraversal<S, ? extends Number> min();
    AtlasGraphTraversal<S, ? extends Number> max();
    AtlasGraphTraversal<S, ? extends Number> sum();

    /*
        Side-effect expressions
     */
    AtlasGraphTraversal<S, ?> cap(String namedStep);
    AtlasGraphTraversal<S, ?> cap(int numberedStep);
    AtlasGraphTraversal<S, E> dedup();
    AtlasGraphTraversal<S, E> filter(final Function filterFunction);
    AtlasGraphTraversal<S, E> except(final Function exceptFunction);
    AtlasGraphTraversal<S, E> except(final Collection<?> exclude);
    AtlasGraphTraversal<S, ?> ifThenElse(final Function ifFunc, final Function thenFunc, final Function elseFunc);
    <T> AtlasGraphTraversal<S, T> transform(final Function transformFunction);
    <T> AtlasGraphTraversal<S, Map<String, T>> select(final String ... keys);
    <T> AtlasGraphTraversal<S, Map<String, T>> select(final Function... selectFunction);

    /*
        Aggregation
     */
    AtlasGraphTraversal<S, E> aggregate();
    AtlasGraphTraversal<S, E> aggregate(final Collection<E> aggregate);
    AtlasGraphTraversal<S, E> aggregate(final Collection<E> aggregate, final Function aggregateFunction);
    AtlasGraphTraversal<S, E> aggregate(final Function aggregateFunction);

    /*
        Grouping
     */
    AtlasGraphTraversal<S, E> groupBy(final Function keyFunction, final Function valueFunction);
    AtlasGraphTraversal<S, E> groupBy(final Function keyFunction, final Function valueFunction, final Function reduceFunction);
    AtlasGraphTraversal<S, E> groupCount();
    AtlasGraphTraversal<S, E> groupCount(final Function keyFunction);

    /*
        Looping and back reference
     */
    AtlasGraphTraversal<S, ?> back(final String namedStep);
    AtlasGraphTraversal<S, E> loop(final String namedStep, final Function whileFunction);
    AtlasGraphTraversal<S, E> loop(final String namedStep, final Function whileFunction, final Function emitFunction);
    AtlasGraphTraversal<S, E> as(final String name);

    /*
        Ordering
     */
    AtlasGraphTraversal<S, E> order();
    AtlasGraphTraversal<S, E> order(final Order order);
    AtlasGraphTraversal<S, E> orderBy(final Comparator comparator);

    /*
        Path
     */
    AtlasGraphTraversal<S, E> path(final Function... pathFunctions);
    AtlasGraphTraversal<S, E> simplePath();


    <A, B> AtlasGraphTraversal<S, E> sideEffect(final Function<A, B> sideEffectFunction);
    AtlasGraphTraversal<S, E> values();

    /*
        Anonymous traversal expression
     */
    AtlasGraphTraversal<S, E> _();
    <A,B> AtlasGraphTraversal<S, ?> step(Function<A, B> stepFunction);
    <C> AtlasGraphTraversal<S, E> cast(Class<C> end);

    long count();

    /*
        Collect expressions (mostly the end step of any query)
     */
    void fill(Collection<E> toFill);
    List toList();
    Set toSet();
}
