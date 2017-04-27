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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.atlas.ibmgraph.IBMGraphEdge;
import org.apache.atlas.ibmgraph.IBMGraphGraph;
import org.apache.atlas.ibmgraph.IBMGraphVertex;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery.ComparisionOperator;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Before;
import org.testng.annotations.Test;


/**
 *
 */
public class GraphQueryTest extends AbstractGraphDatabaseTest {

    @Before
    public <V,E> void clearGraph() {
        AtlasGraph<V,E> graph = getGraph();
        graph.clear();
    }

    private AtlasGraphQuery<IBMGraphVertex, IBMGraphEdge> g_dot_V() {
        return getIbmGraphGraph().query();
    }

    @Test
    public <V,E> void testQueryThatCannotRunInMemory() {
        AtlasGraph<V,E> graph = getGraph();
        graph.clear();
        AtlasVertex<V,E> v1 = createVertex(graph);

        v1.setProperty("name", "Fred");
        v1.setProperty("size15", "15");

        AtlasVertex<V,E> v2 = createVertex(graph);
        v2.setProperty("name", "Fred");

        AtlasVertex<V,E> v3 = createVertex(graph);
        v3.setProperty("size15", "15");

        graph.commit();

        AtlasVertex<V,E> v4 = createVertex(graph);
        v4.setProperty("name", "Fred");
        v4.setProperty("size15", "15");

        AtlasGraphQuery q = graph.query();
        q.has("name", ComparisionOperator.NOT_EQUAL, "George");
        q.has("size15","15");
        graph.commit();
        pause(); //pause to let the index get updated

        assertQueryMatches(q, v1, v3, v4);

    }



    @Test
    public  void testCombinationOfAndsAndOrs() {
        IBMGraphGraph graph = getIbmGraphGraph();
        graph.clear();
        AtlasVertex<IBMGraphVertex,IBMGraphEdge> v1 = createVertex(graph);

        v1.setProperty("name", "Fred");
        v1.setProperty("size15", "15");
        v1.setProperty("typeName", "Person");

        AtlasVertex<IBMGraphVertex,IBMGraphEdge> v2 = createVertex(graph);
        v2.setProperty("name", "George");
        v2.setProperty("size15", "16");
        v2.setProperty("typeName", "Person");

        AtlasVertex<IBMGraphVertex,IBMGraphEdge> v3 = createVertex(graph);
        v3.setProperty("name", "Jane");
        v3.setProperty("size15", "17");
        v3.setProperty("typeName", "Person");


        AtlasVertex<IBMGraphVertex,IBMGraphEdge> v4 = createVertex(graph);
        v4.setProperty("name", "Bob");
        v4.setProperty("size15", "18");
        v4.setProperty("typeName", "Person");

        AtlasVertex<IBMGraphVertex,IBMGraphEdge> v5 = createVertex(graph);
        v5.setProperty("name", "Julia");
        v5.setProperty("size15", "19");
        v5.setProperty("typeName", "Manager");


        AtlasGraphQuery q = g_dot_V();
        q.has("typeName","Person");
        //initially match
        AtlasGraphQuery inner1a = q.createChildQuery();
        AtlasGraphQuery inner1b = q.createChildQuery();
        inner1a.has("name","Fred");
        inner1b.has("name","Jane");
        q.or(toList(inner1a, inner1b));


        AtlasGraphQuery inner2a = q.createChildQuery();
        AtlasGraphQuery inner2b = q.createChildQuery();
        AtlasGraphQuery inner2c = q.createChildQuery();
        inner2a.has("size15","18");
        inner2b.has("size15","15");
        inner2c.has("size15", "16");
        q.or(toList(inner2a, inner2b, inner2c));

        assertQueryMatches(q, v1);
        graph.commit();
        pause(); //let the index update
        assertQueryMatches(q, v1);
    }

    @Test
    public  void testWithinStep() {
        IBMGraphGraph graph = getIbmGraphGraph();
        graph.clear();
        AtlasVertex<IBMGraphVertex,IBMGraphEdge> v1 = createVertex(graph);

        v1.setProperty("name", "Fred");
        v1.setProperty("size15", "15");
        v1.setProperty("typeName", "Person");

        AtlasVertex<IBMGraphVertex,IBMGraphEdge> v2 = createVertex(graph);
        v2.setProperty("name", "George");
        v2.setProperty("size15", "16");
        v2.setProperty("typeName", "Person");

        AtlasVertex<IBMGraphVertex,IBMGraphEdge> v3 = createVertex(graph);
        v3.setProperty("name", "Jane");
        v3.setProperty("size15", "17");
        v3.setProperty("typeName", "Person");


        AtlasVertex<IBMGraphVertex,IBMGraphEdge> v4 = createVertex(graph);
        v4.setProperty("name", "Bob");
        v4.setProperty("size15", "18");
        v4.setProperty("typeName", "Person");

        AtlasVertex<IBMGraphVertex,IBMGraphEdge> v5 = createVertex(graph);
        v5.setProperty("name", "Julia");
        v5.setProperty("size15", "19");
        v5.setProperty("typeName", "Manager");


        AtlasGraphQuery q = g_dot_V();
        q.has("typeName","Person");
        //initially match
        q.in("name", toList("Fred", "Jane"));
        q.in("size15", toList("18", "15", "16"));

        assertQueryMatches(q, v1);
        graph.commit();
        pause(); //let the index update
        assertQueryMatches(q, v1);
    }

    @Test
    public  void testWithinStepWhereGraphIsStale() {
        IBMGraphGraph graph = getIbmGraphGraph();
        graph.clear();
        AtlasVertex<IBMGraphVertex,IBMGraphEdge> v1 = createVertex(graph);

        v1.setProperty("name", "Fred");
        v1.setProperty("size15", "15");
        v1.setProperty("typeName", "Person");

        AtlasVertex<IBMGraphVertex,IBMGraphEdge> v2 = createVertex(graph);
        v2.setProperty("name", "George");
        v2.setProperty("size15", "16");
        v2.setProperty("typeName", "Person");

        AtlasVertex<IBMGraphVertex,IBMGraphEdge> v3 = createVertex(graph);
        v3.setProperty("name", "Jane");
        v3.setProperty("size15", "17");
        v3.setProperty("typeName", "Person");


        AtlasVertex<IBMGraphVertex,IBMGraphEdge> v4 = createVertex(graph);
        v4.setProperty("name", "Bob");
        v4.setProperty("size15", "18");
        v4.setProperty("typeName", "Person");

        AtlasVertex<IBMGraphVertex,IBMGraphEdge> v5 = createVertex(graph);
        v5.setProperty("name", "Julia");
        v5.setProperty("size15", "19");
        v5.setProperty("typeName", "Manager");


        AtlasGraphQuery q = g_dot_V();
        q.has("typeName","Person");
        //initially match
        q.in("name", toList("Fred", "Jane"));

        graph.commit();
        pause(); //let the index update
        assertQueryMatches(q, v1, v3);
        v3.setProperty("name", "Janet"); //make v3 no longer match the query.  Within step should filter out the vertex since it no longer matches.
        assertQueryMatches(q, v1);
    }

    @Test
    public  void testSimpleOrQuery() {
        IBMGraphGraph graph = getIbmGraphGraph();
        graph.clear();
        AtlasVertex<IBMGraphVertex,IBMGraphEdge> v1 = createVertex(graph);

        v1.setProperty("name", "Fred");
        v1.setProperty("size15", "15");

        AtlasVertex<IBMGraphVertex,IBMGraphEdge> v2 = createVertex(graph);
        v2.setProperty("name", "Fred");

        AtlasVertex<IBMGraphVertex,IBMGraphEdge> v3 = createVertex(graph);
        v3.setProperty("size15", "15");

        graph.commit();

        AtlasVertex<IBMGraphVertex,IBMGraphEdge> v4 = createVertex(graph);
        v4.setProperty("name", "Fred");
        v4.setProperty("size15", "15");

        AtlasVertex<IBMGraphVertex,IBMGraphEdge> v5 = createVertex(graph);
        v5.setProperty("name", "George");
        v5.setProperty("size15", "16");

        AtlasGraphQuery q = graph.query();
        AtlasGraphQuery inner1 = q.createChildQuery();
        inner1.has("name", "Fred");

        AtlasGraphQuery inner2 = q.createChildQuery();
        inner2.has("size15", "15");
        q.or(toList(inner1, inner2));
        assertQueryMatches(q, v1, v2, v3, v4);
        graph.commit();
        pause(); //pause to let the indexer get updated (this fails frequently without a pause)
        assertQueryMatches(q, v1, v2, v3, v4);
    }

    private void pause() {
        try {
            Thread.sleep(5000);
        }
        catch(InterruptedException e)
        {}
    }


    @Test
    public <V,E> void testQueryMatchesAddedVertices() {
        AtlasGraph<V,E> graph = getGraph();
        graph.clear();
        AtlasVertex<V,E> v1 = createVertex(graph);

        v1.setProperty("name", "Fred");
        v1.setProperty("size15", "15");

        AtlasVertex<V,E> v2 = createVertex(graph);
        v2.setProperty("name", "Fred");

        AtlasVertex<V,E> v3 = createVertex(graph);
        v3.setProperty("size15", "15");

        graph.commit();

        AtlasVertex<V,E> v4 = createVertex(graph);
        v4.setProperty("name", "Fred");
        v4.setProperty("size15", "15");

        AtlasGraphQuery q = g_dot_V();
        q.has("name", "Fred");
        q.has("size15","15");

        assertQueryMatches(q, v1, v4);
        graph.commit();
        assertQueryMatches(q, v1, v4);

    }


    @Test
    public <V,E> void testQueryDoesNotMatchRemovedVertices() {
        AtlasGraph<V,E> graph = getGraph();

        AtlasVertex<V,E> v1 = createVertex(graph);

        v1.setProperty("name", "Fred");
        v1.setProperty("size15", "15");

        AtlasVertex<V,E> v2 = createVertex(graph);
        v2.setProperty("name", "Fred");

        AtlasVertex<V,E> v3 = createVertex(graph);
        v3.setProperty("size15", "15");

        AtlasVertex<V,E> v4 = createVertex(graph);
        v4.setProperty("name", "Fred");
        v4.setProperty("size15", "15");

        graph.commit();

        graph.removeVertex(v1);

        AtlasGraphQuery q = g_dot_V();
        q.has("name", "Fred");
        q.has("size15","15");

        assertQueryMatches(q, v4);
        graph.commit();

        assertQueryMatches(q, v4);
    }

    @Test
    public <V,E> void testQueryDoesNotMatchUncommittedAddedAndRemovedVertices() {
        AtlasGraph<V,E> graph = getGraph();

        AtlasVertex<V,E> v1 = createVertex(graph);

        v1.setProperty("name", "Fred");
        v1.setProperty("size15", "15");

        AtlasVertex<V,E> v2 = createVertex(graph);
        v2.setProperty("name", "Fred");

        AtlasVertex<V,E> v3 = createVertex(graph);
        v3.setProperty("size15", "15");

        AtlasVertex<V,E> v4 = createVertex(graph);
        v4.setProperty("name", "Fred");
        v4.setProperty("size15", "15");


        AtlasGraphQuery q = g_dot_V();
        q.has("name", "Fred");
        q.has("size15","15");

        assertQueryMatches(q, v1, v4);

        graph.removeVertex(v1);


        assertQueryMatches(q, v4);
        graph.commit();

        assertQueryMatches(q, v4);
    }

    @Test
    public <V,E> void testQueryResultsReflectPropertyChanges() {
        AtlasGraph<V,E> graph = getGraph();

        AtlasVertex<V,E> v1 = createVertex(graph);
        v1.setProperty("name", "Fred");
        v1.setProperty("size15", "15");

        AtlasVertex<V,E> v2 = createVertex(graph);
        v2.setProperty("name", "Fred");

        AtlasVertex<V,E> v3 = createVertex(graph);
        v3.setProperty("size15", "15");

        AtlasVertex<V,E> v4 = createVertex(graph);
        v4.setProperty("name", "Fred");
        v4.setProperty("size15", "15");

        AtlasVertex<V,E> v5 = createVertex(graph);
        v5.setProperty("name", "George");
        v5.setProperty("size15", "15");

        graph.commit();

        //new property
        v2.setProperty("size15", "15"); //set new property.  Now v2 should match query
        v5.setProperty("name", "Fred"); //change property value.  now v5 should match query
        v1.setProperty("name", "George"); //change property value.  now v1 should not match

        AtlasGraphQuery q = g_dot_V();
        q.has("name", "Fred");
        q.has("size15","15");

        assertQueryMatches(q, v2, v4, v5);
        graph.commit();
        assertQueryMatches(q, v2, v4, v5);

        v1.setProperty("name", "Fred");
        assertQueryMatches(q, v1, v2, v4, v5);

        v1.setProperty("name", "Bob");

        assertQueryMatches(q, v2, v4, v5);
        graph.commit();
        assertQueryMatches(q, v2, v4, v5);
    }

    @Test
    public <V,E> void testQueryResultsReflectPropertyRemoval() {
        AtlasGraph<V,E> graph = getGraph();

        AtlasVertex<V,E> v1 = createVertex(graph);
        v1.setProperty("name", "Fred");
        v1.setProperty("size15", "15");

        AtlasVertex<V,E> v2 = createVertex(graph);
        v2.setProperty("name", "Fred");

        AtlasVertex<V,E> v3 = createVertex(graph);
        v3.setProperty("size15", "15");

        AtlasVertex<V,E> v4 = createVertex(graph);
        v4.setProperty("name", "Fred");
        v4.setProperty("size15", "15");

        AtlasVertex<V,E> v5 = createVertex(graph);
        v5.setProperty("name", "George");
        v5.setProperty("size15", "15");

        AtlasGraphQuery q = g_dot_V();
        q.has("name", "Fred");
        q.has("size15","15");

        graph.commit();

        assertQueryMatches(q, v1, v4);

        //new property
        v2.setProperty("size15", "15"); //set new property.  Now v2 should match query
        v5.setProperty("name", "Fred"); //change property value.  now v5 should match query

        assertQueryMatches(q, v1, v2, v4, v5);
        v5.removeProperty("name");
        assertQueryMatches(q, v1, v2, v4);
        graph.commit();
        assertQueryMatches(q, v1, v2, v4);

        v2.removeProperty("name");
        assertQueryMatches(q, v1, v4);
        v4.removeProperty("size15");
        assertQueryMatches(q, v1);
        v1.removeProperty("size15");
        assertQueryMatches(q); //should be no results now
        graph.commit();
        assertQueryMatches(q);
    }

    @Test
    public <V,E> void testQueryResultsReflectPropertyAdd() {
        AtlasGraph<V,E> graph = getGraph();

        AtlasVertex<V,E> v1 = createVertex(graph);
        v1.setProperty("name", "Fred");
        v1.setProperty("size15", "15");
        v1.addProperty(TRAIT_NAMES, "trait1");
        v1.addProperty(TRAIT_NAMES, "trait2");

        AtlasVertex<V,E> v2 = createVertex(graph);
        v2.setProperty("name", "Fred");
        v2.addProperty(TRAIT_NAMES, "trait1");

        AtlasVertex<V,E> v3 = createVertex(graph);
        v3.setProperty("size15", "15");
        v3.addProperty(TRAIT_NAMES, "trait2");

        AtlasGraphQuery query = g_dot_V();
        query.has("name", "Fred");
        query.has(TRAIT_NAMES, "trait1");
        query.has("size15", "15");

        assertQueryMatches(query, v1);
        //make v3 match the query
        v3.setProperty("name", "Fred");
        v3.addProperty(TRAIT_NAMES, "trait1");
        assertQueryMatches(query, v1, v3);
        v3.removeProperty(TRAIT_NAMES);
        assertQueryMatches(query, v1);
        v3.addProperty(TRAIT_NAMES, "trait2");
        assertQueryMatches(query, v1);
        v1.removeProperty(TRAIT_NAMES);
        assertQueryMatches(query);
        graph.commit();
        assertQueryMatches(query);

    }

    private <V,E >void assertQueryMatches(AtlasGraphQuery expr, AtlasVertex... expectedResults) {

        Collection<AtlasVertex<IBMGraphVertex, IBMGraphEdge>> result = IteratorUtils.asList(expr.vertices());
        assertEquals(expectedResults.length, result.size(), "Expected/found result sizes differ.  Expected: " + Arrays.asList(expectedResults).toString() +", found: " + result);
        for(AtlasVertex<V,E> v : expectedResults) {
            assertTrue(result.contains(v));
        }
    }

    private static List<Object> toList(Object...objects) {
        return Arrays.asList(objects);
    }
}
