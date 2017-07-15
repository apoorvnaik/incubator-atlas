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

import org.apache.atlas.repository.graphdb.AtlasGraphQuery.QueryOperator;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface AtlasMultiVertexQuery<V, E, Q extends AtlasMultiVertexQuery> {
    Q addVertex(AtlasVertex<V, E> vertex);
    Q addAllVertice(Collection<AtlasVertex<V, E>> vertex);
    Q labels(String ... labels);
    Q keys(String ... keys);
    Q direction(AtlasEdgeDirection direction);

    Q has(String key);
    Q hasNot(String key);
    Q has(String key, Object value);
    Q hasNot(String key, Object value);
    Q has(String key, QueryOperator operator, Object value);
    <T extends Comparable<?>> Q interval(String key, T start, T end);

    Q or(Q ... multiVertexQueries);
    Q or(List<Q> multiVertexQueries);
    Q createChildQuery();
    boolean isChildQuery();

    Q addConditionsFrom(Q otherQuery);

    Q limit(int limit);

    Map<V, Iterable<AtlasPropertyKey>> properties();

    Map<V, Iterable<E>> edges();
    Map<V, Iterable<V>> vertices();
    Iterable<E> edges(boolean flatten);
    Iterable<V> vertices(boolean flatten);

    Map<V, Iterable<E>> edges(int limit);
    Map<V, Iterable<E>> edges(int offset, int limit);
    Map<V, Iterable<V>> vertices(int limit);
    Map<V, Iterable<V>> vertices(int offset, int limit);

    Iterable<E> edges(int limit, boolean flatten);
    Iterable<E> edges(int offset, int limit, boolean flatten);
    Iterable<V> vertices(int limit, boolean flatten);
    Iterable<V> vertices(int offset, int limit, boolean flatten);
}
