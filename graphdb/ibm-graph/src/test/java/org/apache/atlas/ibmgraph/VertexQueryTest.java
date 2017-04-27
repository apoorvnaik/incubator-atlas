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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;


/**
 *
 */
public class VertexQueryTest extends AbstractGraphDatabaseTest {

    @Test
    public <V,E> void testIncomingEdgesFromGraphCached() {
        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> v1 = createVertex(graph);
        AtlasVertex<V, E> v2 = createVertex(graph);
        AtlasVertex<V, E> v3 = createVertex(graph);

        AtlasEdge<V,E> e1 = graph.addEdge(v1, v2, "eats");
        AtlasEdge<V,E> e2 = graph.addEdge(v2, v3, "eats");
        AtlasEdge<V,E> e3 = graph.addEdge(v3, v1, "eats");
        AtlasEdge<V,E> e4 = graph.addEdge(v3, v1, "knows");

        graph.commit();

        String v1Id = v1.getId().toString();

        pushChangesAndFlushCache();
        Iterable<AtlasEdge<V,E>> inEdges = v1.getEdges(AtlasEdgeDirection.IN);
        graph.removeVertex(v1);

        for(AtlasEdge<V,E> edge : inEdges) {
            assertFalse(edge.exists());
        }
    }


    @Test
    public <V,E> void testMergeGraphAndUnCommittedEdges() {
        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> v1 = createVertex(graph);
        AtlasVertex<V, E> v2 = createVertex(graph);
        AtlasVertex<V, E> v3 = createVertex(graph);

        AtlasEdge<V,E> e1 = graph.addEdge(v1, v2, "eats");
        AtlasEdge<V,E> e2 = graph.addEdge(v2, v3, "eats");
        AtlasEdge<V,E> e3 = graph.addEdge(v3, v1, "eats");
        AtlasEdge<V,E> e4 = graph.addEdge(v3, v1, "knows");


        assertEdgesMatch(v1, null, toArray(e1), toArray(e3, e4));
        assertEdgesMatch(v1, "eats",  toArray(e1), toArray(e3));
        assertEdgesMatch(v1, "knows", toArray(), toArray(e4));

        assertEdgesMatch(v2, null, toArray(e2), toArray(e1));
        assertEdgesMatch(v2, "eats", toArray(e2), toArray(e1));
        assertEdgesMatch(v2, "knows", toArray(), toArray());

        assertEdgesMatch(v3, null, toArray(e3, e4), toArray(e2));
        assertEdgesMatch(v3, "eats", toArray(e3), toArray(e2));
        assertEdgesMatch(v3, "knows", toArray(e4), toArray());

        graph.commit();

        assertEdgesMatch(v1, null, toArray(e1), toArray(e3, e4));
        assertEdgesMatch(v1, "eats",  toArray(e1), toArray(e3));
        assertEdgesMatch(v1, "knows", toArray(), toArray(e4));

        assertEdgesMatch(v2, null, toArray(e2), toArray(e1));
        assertEdgesMatch(v2, "eats", toArray(e2), toArray(e1));
        assertEdgesMatch(v2, "knows", toArray(), toArray());

        assertEdgesMatch(v3, null, toArray(e3, e4), toArray(e2));
        assertEdgesMatch(v3, "eats", toArray(e3), toArray(e2));
        assertEdgesMatch(v3, "knows", toArray(e4), toArray());

        graph.removeEdge(e1);
        AtlasEdge<V,E> e5 = graph.addEdge(v1, v2, "sees");
        AtlasEdge<V,E> e6 = graph.addEdge(v1, v2, "eats");

        assertEdgesMatch(v1, null, toArray(e5, e6), toArray(e3, e4));
        assertEdgesMatch(v1, "sees",  toArray(e5), toArray());
        assertEdgesMatch(v1, "eats",  toArray(e6), toArray(e3));
        assertEdgesMatch(v1, "knows", toArray(), toArray(e4));

        assertEdgesMatch(v2, null, toArray(e2), toArray(e5, e6));
        assertEdgesMatch(v2, "sees", toArray(), toArray(e5));
        assertEdgesMatch(v2, "eats", toArray(e2), toArray(e6));
        assertEdgesMatch(v2, "knows", toArray(), toArray());

        assertEdgesMatch(v3, null, toArray(e3, e4), toArray(e2));
        assertEdgesMatch(v3, "sees", toArray(), toArray());
        assertEdgesMatch(v3, "eats", toArray(e3), toArray(e2));
        assertEdgesMatch(v3, "knows", toArray(e4), toArray());

        graph.commit();

        assertEdgesMatch(v1, null, toArray(e5, e6), toArray(e3, e4));
        assertEdgesMatch(v1, "sees",  toArray(e5), toArray());
        assertEdgesMatch(v1, "eats",  toArray(e6), toArray(e3));
        assertEdgesMatch(v1, "knows", toArray(), toArray(e4));

        assertEdgesMatch(v2, null, toArray(e2), toArray(e5, e6));
        assertEdgesMatch(v2, "sees", toArray(), toArray(e5));
        assertEdgesMatch(v2, "eats", toArray(e2), toArray(e6));
        assertEdgesMatch(v2, "knows", toArray(), toArray());

        assertEdgesMatch(v3, null, toArray(e3, e4), toArray(e2));
        assertEdgesMatch(v3, "sees", toArray(), toArray());
        assertEdgesMatch(v3, "eats", toArray(e3), toArray(e2));
        assertEdgesMatch(v3, "knows", toArray(e4), toArray());

    }

    private static <V,E> AtlasEdge<V,E>[] toArray(AtlasEdge<V,E>... values) {
        return values;
    }

    private <V,E> void assertEdgesMatch(AtlasVertex<V,E> vertex, String label, AtlasEdge<V,E>[] expectedOutEdges, AtlasEdge<V,E>[] expectedInEdges) {

        Set<AtlasEdge<V,E>> expectedInEdgesSet = new HashSet<>(Arrays.asList(expectedInEdges));
        Set<AtlasEdge<V,E>> expectedOutEdgesSet = new HashSet<>(Arrays.asList(expectedOutEdges));
        Set<AtlasEdge<V,E>> expectedBothEdgesSet = Sets.union(expectedInEdgesSet, expectedOutEdgesSet);

        Set<AtlasVertex<V,E>> expectedInVertices = new HashSet<>();
        for(AtlasEdge<V,E> inEdge : expectedInEdges) {
            expectedInVertices.add(inEdge.getOutVertex());
        }

        Set<AtlasVertex<V,E>> expectedOutVertices = new HashSet<>();
        for(AtlasEdge<V,E> outEdge : expectedOutEdges) {
            expectedOutVertices.add(outEdge.getInVertex());
        }

        Collection<AtlasVertex<V,E>> expectedBothVertices = new HashSet<>();
        expectedBothVertices.addAll(expectedInVertices);
        expectedBothVertices.addAll(expectedOutVertices);

        if(label == null) {

            assertContentsSame(expectedInEdgesSet, vertex.query().direction(AtlasEdgeDirection.IN).edges());
            assertContentsSame(expectedOutEdgesSet, vertex.query().direction(AtlasEdgeDirection.OUT).edges());
            assertContentsSame(expectedBothEdgesSet, vertex.query().direction(AtlasEdgeDirection.BOTH).edges());
            assertContentsSame(expectedBothEdgesSet, vertex.query().edges());

            assertContentsSame(expectedInVertices, vertex.query().direction(AtlasEdgeDirection.IN).vertices());
            assertContentsSame(expectedOutVertices, vertex.query().direction(AtlasEdgeDirection.OUT).vertices());
            assertContentsSame(expectedBothVertices, vertex.query().direction(AtlasEdgeDirection.BOTH).vertices());
            assertContentsSame(expectedBothVertices, vertex.query().vertices());

            assertEquals(expectedInEdgesSet.size(), vertex.query().direction(AtlasEdgeDirection.IN).count());
            assertEquals(expectedOutEdgesSet.size(), vertex.query().direction(AtlasEdgeDirection.OUT).count());
            assertEquals(expectedBothEdgesSet.size(), vertex.query().direction(AtlasEdgeDirection.BOTH).count());
            assertEquals(expectedBothEdgesSet.size(), vertex.query().count());

            assertContentsSame(expectedInEdgesSet, vertex.getEdges(AtlasEdgeDirection.IN));
            assertContentsSame(expectedOutEdgesSet, vertex.getEdges(AtlasEdgeDirection.OUT));
            assertContentsSame(expectedBothEdgesSet, vertex.getEdges(AtlasEdgeDirection.BOTH));

        }
        else {

            //graph db api does not currently support vertex queries with labels

            assertContentsSame(expectedInEdgesSet, vertex.getEdges(AtlasEdgeDirection.IN, label));
            assertContentsSame(expectedOutEdgesSet, vertex.getEdges(AtlasEdgeDirection.OUT, label));
            assertContentsSame(expectedBothEdgesSet, vertex.getEdges(AtlasEdgeDirection.BOTH, label));
        }
    }

    private <T> void assertContentsSame(Collection<T> expected, Iterable<T> found) {
        assertContentsSame(expected, IteratorUtils.asList(found));
    }

    private <T> void assertContentsSame(Collection<T> expected, Collection<T> found) {
        assertEquals(expected.size(), found.size());
        for(T item : expected) {
            assertTrue(found.contains(item));
        }
    }


    @Test
    public <V, E> void testVertexQuery () {
        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> v1 = createVertex(graph);
        AtlasVertex<V, E> v2 = createVertex(graph);
        AtlasVertex<V, E> v3 = createVertex(graph);

        v1.addProperty(typeProperty, typeSystem);
        v2.addProperty(typeProperty, typeSystem);
        v3.addProperty(typeProperty, typeSystem);
        AtlasEdge<V, E> knows = graph.addEdge(v2, v1, "knows");
        AtlasEdge<V, E> eats = graph.addEdge(v1, v3, "eats"); //V2-->v1 -->v3

        Iterable<AtlasVertex<V, E>> vertices = v1.query().direction(AtlasEdgeDirection.IN).vertices();
        assertResultElementsContainElement(vertices, (String)v2.getId());
        vertices = v1.query().direction(AtlasEdgeDirection.OUT).vertices();
        assertResultElementsContainElement(vertices, (String)v3.getId());
        vertices = v1.query().direction(AtlasEdgeDirection.BOTH).vertices();
        assertResultElementsContainElement(vertices, (String)v2.getId());
        assertResultElementsContainElement(vertices, (String)v3.getId());

        Iterable<AtlasEdge<V, E>> edges = v1.query().direction(AtlasEdgeDirection.IN).edges();
        assertResultElementsContainElement(edges, (String)knows.getId());
        edges = v1.query().direction(AtlasEdgeDirection.BOTH).edges();
        assertResultElementsContainElement(edges, (String)knows.getId());
        assertResultElementsContainElement(edges, (String)eats.getId());
        edges = v1.query().direction(AtlasEdgeDirection.OUT).edges();
        assertResultElementsContainElement(edges, (String)eats.getId());

        long count = v2.query().direction(AtlasEdgeDirection.IN).count();
        assertEquals(0, count);
        count = v1.query().direction(AtlasEdgeDirection.IN).count();
        assertEquals(1, count);
        count = v1.query().direction(AtlasEdgeDirection.OUT).count();
        assertEquals(1, count);
        count = v1.query().direction(AtlasEdgeDirection.BOTH).count();
        assertEquals(2, count);

        Iterable<AtlasVertex<V, E>> vertices1 = graph.getVertices(typeProperty, typeSystem);
        assertResultElementsContainElement(vertices1, (String) v1.getId());
        assertResultElementsContainElement(vertices1, (String) v2.getId());
        assertResultElementsContainElement(vertices1, (String) v3.getId());

        graph.removeEdge(knows);
        graph.removeEdge(eats);

    }


    @Test
    public <V,E> void testEdgeRemovalsReflectedInQueryBeforeCommit() {

        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> inV = createVertex(graph);
        AtlasVertex<V, E> outV = createVertex(graph);

        AtlasEdge<V,E> e1 = graph.addEdge(outV, inV, "eats");
        graph.commit();

        assertEdgesMatch(inV, null, toArray(), toArray(e1));
        assertEdgesMatch(inV, "eats", toArray(), toArray(e1));
        assertEdgesMatch(inV, "sleeps", toArray(), toArray());

        assertEdgesMatch(outV, null, toArray(e1), toArray());
        assertEdgesMatch(outV, "eats", toArray(e1), toArray());
        assertEdgesMatch(outV, "sleeps", toArray(), toArray());



        //does not get pushed to graph until pending changes are applied.  Verify that
        //removal is reflected in graph query
        graph.removeEdge(e1);

        assertEdgesMatch(inV, null, toArray(), toArray());
        assertEdgesMatch(inV, "eats", toArray(), toArray());
        assertEdgesMatch(inV, "sleeps", toArray(), toArray());

        assertEdgesMatch(outV, null, toArray(), toArray());
        assertEdgesMatch(outV, "eats", toArray(), toArray());
        assertEdgesMatch(outV, "sleeps", toArray(), toArray());

        graph.commit();

        assertEdgesMatch(inV, null, toArray(), toArray());
        assertEdgesMatch(inV, "eats", toArray(), toArray());
        assertEdgesMatch(inV, "sleeps", toArray(), toArray());

        assertEdgesMatch(outV, null, toArray(), toArray());
        assertEdgesMatch(outV, "eats", toArray(), toArray());
        assertEdgesMatch(outV, "sleeps", toArray(), toArray());

    }
    @Test
    public <V,E> void testInVertexRemovalReflectedInQueryBeforeCommit() {

        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> inV = createVertex(graph);
        AtlasVertex<V, E> outV = createVertex(graph);

        AtlasEdge<V,E> e1 = graph.addEdge(outV, inV, "eats");
        graph.commit();

        assertEdgesMatch(inV, null, toArray(), toArray(e1));
        assertEdgesMatch(inV, "eats", toArray(), toArray(e1));
        assertEdgesMatch(inV, "sleeps", toArray(), toArray());

        assertEdgesMatch(outV, null, toArray(e1), toArray());
        assertEdgesMatch(outV, "eats", toArray(e1), toArray());
        assertEdgesMatch(outV, "sleeps", toArray(), toArray());


        //does not get pushed to graph until pending changes are applied.  Verify that
        //removal is reflected in graph query
        graph.removeVertex(inV);

        assertEdgesMatch(outV, null, toArray(), toArray());
        assertEdgesMatch(outV, "eats", toArray(), toArray());
        assertEdgesMatch(outV, "sleeps", toArray(), toArray());
        graph.commit();

        assertEdgesMatch(outV, null, toArray(), toArray());
        assertEdgesMatch(outV, "eats", toArray(), toArray());
        assertEdgesMatch(outV, "sleeps", toArray(), toArray());
    }


    @Test
    public <V,E> void testAddAndRemoveVertexBeforeCommit() {

        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> inV = createVertex(graph);
        AtlasVertex<V, E> outV = createVertex(graph);

        AtlasEdge<V,E> e1 = graph.addEdge(outV, inV, "eats");

        assertEdgesMatch(inV, null, toArray(), toArray(e1));
        assertEdgesMatch(inV, "eats", toArray(), toArray(e1));
        assertEdgesMatch(inV, "sleeps", toArray(), toArray());

        assertEdgesMatch(outV, null, toArray(e1), toArray());
        assertEdgesMatch(outV, "eats", toArray(e1), toArray());
        assertEdgesMatch(outV, "sleeps", toArray(), toArray());

        //does not get pushed to graph until pending changes are applied.  Verify that
        //removal is reflected in graph query
        graph.removeVertex(inV);

        assertEdgesMatch(outV, null, toArray(), toArray());
        assertEdgesMatch(outV, "eats", toArray(), toArray());
        assertEdgesMatch(outV, "sleeps", toArray(), toArray());

        graph.commit();

        assertEdgesMatch(outV, null, toArray(), toArray());
        assertEdgesMatch(outV, "eats", toArray(), toArray());
        assertEdgesMatch(outV, "sleeps", toArray(), toArray());

    }

    @Test
    public <V,E> void testOutVertexRemovalsReflectedInQueryBeforeCommit() {

        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> inV = createVertex(graph);
        AtlasVertex<V, E> outV = createVertex(graph);

        AtlasEdge<V,E> e1 = graph.addEdge(outV, inV, "eats");
        graph.commit();

        assertEdgesMatch(inV, null, toArray(), toArray(e1));
        assertEdgesMatch(inV, "eats", toArray(), toArray(e1));
        assertEdgesMatch(inV, "sleeps", toArray(), toArray());

        assertEdgesMatch(outV, null, toArray(e1), toArray());
        assertEdgesMatch(outV, "eats", toArray(e1), toArray());
        assertEdgesMatch(outV, "sleeps", toArray(), toArray());


        //does not get pushed to graph until pending changes are applied.  Verify that
        //removal is reflected in graph query
        graph.removeVertex(outV);

        assertEdgesMatch(inV, null, toArray(), toArray());
        assertEdgesMatch(inV, "eats", toArray(), toArray());
        assertEdgesMatch(inV, "sleeps", toArray(), toArray());

        graph.commit();

        assertEdgesMatch(inV, null, toArray(), toArray());
        assertEdgesMatch(inV, "eats", toArray(), toArray());
        assertEdgesMatch(inV, "sleeps", toArray(), toArray());

    }

    @Test
    public <V,E> void testInAndOutVertexRemovalsReflectedInQueryBeforeCommit() {

        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> inV = createVertex(graph);
        AtlasVertex<V, E> outV = createVertex(graph);

        AtlasEdge<V,E> e1 = graph.addEdge(outV, inV, "eats");
        graph.commit();

        assertEdgesMatch(inV, null, toArray(), toArray(e1));
        assertEdgesMatch(inV, "eats", toArray(), toArray(e1));
        assertEdgesMatch(inV, "sleeps", toArray(), toArray());

        assertEdgesMatch(outV, null, toArray(e1), toArray());
        assertEdgesMatch(outV, "eats", toArray(e1), toArray());
        assertEdgesMatch(outV, "sleeps", toArray(), toArray());

        //does not get pushed to graph until pending changes are applied.  Verify that
        //removal is reflected in graph query
        graph.removeVertex(outV);
        graph.removeVertex(inV);

        graph.commit();
    }



    private <V, E> void assertResultElementsContainElement(Iterable<? extends AtlasElement> vertices, String expectedElementId) {
        boolean vertexFound = false;
        Iterator<AtlasElement> vIter = (Iterator<AtlasElement>) vertices.iterator();
        while (vIter.hasNext()) {
            AtlasElement v = vIter.next();
            if (v.getId().equals(expectedElementId)) {
                vertexFound = true;
                break;
            }
        }
        assertTrue(vertexFound);
    }

}
