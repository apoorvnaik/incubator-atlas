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
package org.apache.atlas.ibmgraph.tx;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.atlas.ibmgraph.AbstractGraphDatabaseTest;
import org.apache.atlas.ibmgraph.IBMGraphEdge;
import org.apache.atlas.ibmgraph.IBMGraphGraph;
import org.apache.atlas.ibmgraph.IBMGraphVertex;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.commons.collections.IteratorUtils;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import junit.framework.Assert;
/**
 * Tests handling of multi-user scenarios
 */
public class TransactionMultiUserTest extends AbstractGraphDatabaseTest {

    @Test
    public <V,E >void testCommitIsAtomic() throws Throwable {

        //In this test, we pre-create a bunch of vertices and edges.  Then, each thread
        //sets the same properties (p0...p[propertyCount-1]) to a thread-specific value on all of
        //the pre-created vertices.  All of the threads use these same vertices.  Then we commit.
        //We then block until all threads have finished their commit.
        //Finally, we test to ensure that either all of the property changes we made
        //took effect or none of them did.  We don't know which case a given thread
        //will be in in advance.  The last thread to commit the properties should win,
        //and all the threads should end up seeing the values that it set.

        final AtomicInteger counter = new AtomicInteger(0);
        final int threadCount = 5;
        final int vertexCount = 5;
        final int propertyCount = 10;

        List<AtlasVertex<V,E>> vertices = new ArrayList<>();
        AtlasGraph<V,E> graph = getGraph();
        for(int i = 0; i < vertexCount; i++) {
            vertices.add(graph.addVertex());
        }

        List<AtlasEdge<V,E>> edges = new ArrayList<>();
        for(int i = 0; i < vertices.size() - 2; i++) {
            AtlasVertex<V,E> v1 = vertices.get(i);
            AtlasVertex<V,E> v2 = vertices.get(i+1);
            AtlasEdge<V,E> edge = graph.addEdge(v1, v2, "e" + i);
            edges.add(edge);
        }

        graph.commit();

        final List<String> vertexIds = getIds(vertices);
        final List<String> edgeIds = getIds(edges);

        pushChangesAndFlushCache();

        Runnable r = new Runnable() {

            @Override
            public void run() {

                boolean incremented = false;
                try {
                    final String propertyValue = Thread.currentThread().getName();
                    List<AtlasElement> loadedElements = new ArrayList<>(vertexIds.size() + edgeIds.size());
                    for(int i = 0; i < vertexIds.size(); i++) {
                        loadedElements.add(graph.getVertex(vertexIds.get(i)));
                    }

                    for(int i = 0; i < edgeIds.size(); i++) {
                        loadedElements.add(graph.getEdge(edgeIds.get(i)));
                    }
                    for(AtlasElement element : loadedElements) {
                        for(int i = 0; i < propertyCount; i++) {
                            element.setProperty("p" + i, propertyValue);
                        }
                    }
                    graph.commit();

                    //wait for all threads to get here, so that threads won't be modifying the
                    //properties while we're checking them.
                    counter.incrementAndGet();
                    incremented=true;
                    while(counter.get() < threadCount) {
                        try {
                            Thread.sleep(1);
                        }
                        catch(InterruptedException e) {

                        }
                    }
                    //now, check that either all properties were updated or none of them were
                    String firstValue = loadedElements.get(0).getProperty("p0",String.class);
                    boolean shouldMatch = firstValue.equals(propertyValue);
                    for(AtlasElement element : loadedElements) {
                        for(int i = 0; i < propertyCount; i++) {

                            assertEquals(shouldMatch, propertyValue.equals(element.getProperty("p" + i, String.class)), "Property values are wrong for element");
                        }
                    }

                }
                finally {
                    if(!incremented) {
                        //make sure counter gets incremented in the event of failure, so the other threads
                        //do not deadlock waiting for the counter to reach the number of threads
                        counter.incrementAndGet();
                    }
                }
            }
        };

        runInParallelInNewThreads(r, threadCount);
    }


    @Test
    public void testSimultaneousObjectLoads() throws Throwable {

        //In this test, we pre-create a bunch of vertices and edges, then commit the changes
        //and flush the global graph cache.  Then, in different threads/transactions,
        //we simultaneously load all of those objects.
        //This test is designed to put stress on the Vertex and Edge cache that is maintained in
        //IBMGraphGraph.  Having many Vertices and Edges loaded at the same time causes many
        //simultaneous updates and lookups from those caches.  This test ensures that this
        //does not cause corruption in those caches.

        AtomicInteger counter = new AtomicInteger(0);
        final int threadCount = 20;

        List<AtlasVertex<IBMGraphVertex,IBMGraphEdge>> vertices = new ArrayList<>();
        IBMGraphGraph graph = getIbmGraphGraph();
        for(int i = 0; i < 20; i++) {
            vertices.add(graph.addVertex());
        }

        List<AtlasEdge<IBMGraphVertex,IBMGraphEdge>> edges = new ArrayList<>();
        for(int i = 0; i < vertices.size() - 2; i++) {
            AtlasVertex<IBMGraphVertex,IBMGraphEdge> v1 = vertices.get(i);
            AtlasVertex<IBMGraphVertex,IBMGraphEdge> v2 = vertices.get(i+1);
            AtlasEdge<IBMGraphVertex,IBMGraphEdge> edge = graph.addEdge(v1, v2, "e" + i);
            edges.add(edge);
        }

        graph.commit();

        final List<String> vertexIds = getIds(vertices);
        final List<String> edgeIds = getIds(edges);

        pushChangesAndFlushCache();

        Runnable r = new Runnable() {

            @Override
            public void run() {
                boolean incremented = false;
                try {

                    List<AtlasVertex<IBMGraphVertex,IBMGraphEdge>> loadedVertices = new ArrayList<>(vertexIds.size());
                    List<AtlasEdge<IBMGraphVertex,IBMGraphEdge>> loadedEdges = new ArrayList<>(edgeIds.size());
                    for(int i = 0; i < vertexIds.size(); i++) {
                        loadedVertices.add(graph.getVertex(vertexIds.get(i)));
                    }

                    for(int i = 0; i < edgeIds.size(); i++) {
                        loadedEdges.add(graph.getEdge(edgeIds.get(i)));
                    }

                    IBMGraphGraph ibmGraph = (IBMGraphGraph)graph;
                    Assert.assertEquals(edgeIds.size(), ibmGraph.getEdgeCacheSize());
                    Assert.assertEquals(vertexIds.size(), ibmGraph.getVertexCacheSize());
                    counter.incrementAndGet();
                    incremented = true;
                    while(counter.get() < threadCount) {
                        try {
                            Thread.sleep(1);
                        }
                        catch(InterruptedException e)
                        {

                        }
                    }
                    //check that no phantoms were created
                    for(AtlasVertex<IBMGraphVertex,IBMGraphEdge> loadedVertex : loadedVertices) {
                        assertTrue(loadedVertex == graph.getVertex(loadedVertex.getId().toString()));
                    }
                    for(AtlasEdge<IBMGraphVertex,IBMGraphEdge> loadedEdge : loadedEdges) {
                        assertTrue(loadedEdge == graph.getEdge(loadedEdge.getId().toString()));
                    }
                }
                finally {
                    if(! incremented) {
                        //ensure that counter gets incremented in the event of failure to prevent deadlock while
                        //waiting for all threads to increment the counter.
                        counter.incrementAndGet();
                    }

                }

            }
        };

        runInParallelInNewThreads(r, threadCount);


    }



    @Test
    public <V,E> void testSimultaneousProxyResolution() throws Throwable {

        //In this test, we pre-create a bunch of vertices and then clear out the graph, then load
        //all of those vertices as proxies.  We then have all of the different transactions
        //try to resolve that proxy at the same time.
        //
        //Each Vertex has a field in it which stores the state of that Vertex as well
        //as the various information about the vertex which has been loaded from IBM Graph.
        //This test is designed to put stress on that mechanism by causing many simultaneous requests
        //to come in the inflate the proxy.  Only the first if these requests should succeed.  The rest
        //should notice that proxy resolution is in progress and block until it finishes.  This test
        //ensures that this massive influx of proxy resultion requests does not corrupt the state
        //of the Vertex.


        List<AtlasVertex<IBMGraphVertex,IBMGraphEdge>> vertices = new ArrayList<>();
        AtlasGraph<IBMGraphVertex,IBMGraphEdge> graph = getGraph();
        for(int i = 0; i < 1; i++) {
            AtlasVertex<IBMGraphVertex, IBMGraphEdge> vertex = graph.addVertex();
            vertices.add(vertex);
            vertex.setProperty("prop", "vertex" + i);

        }

        graph.commit();

        final List<String> vertexIds = getIds(vertices);

        pushChangesAndFlushCache();

        AtomicInteger counter = new AtomicInteger(0);
        final int threadCount = 50;
        Runnable r = new Runnable() {

            @Override
            public void run() {

                //wait until all the threads get here, so that the proxy resolutions are grouped
                //very closely together
                counter.incrementAndGet();
                while(counter.get() < threadCount) {
                    try {
                        Thread.sleep(1);
                    }
                    catch(InterruptedException e) {

                    }
                }

                int i = 0;
                for(String vertexId : vertexIds) {
                    IBMGraphVertex vertex = graph.getVertex(vertexId).getV();
                    //force proxy to be resolved
                    assertTrue(vertex.exists());
                    assertEquals("vertex" + i, vertex.getProperty("prop", String.class));
                    assertFalse(vertex.isProxy());

                }
            }
        };

        runInParallelInNewThreads(r, threadCount);
    }


    @Test
    public <V,E> void testTransactionsSimultanouslyDeleteElement() throws Throwable {

        //In this test, we pre-create a bunch of vertices and edges.  Then, in a bunch
        //of different threads/transactions, we load and delete those edges at the same
        //time.
        final int threadCount = 20;

        AtlasGraph<IBMGraphVertex,IBMGraphEdge> graph = getGraph();
        IBMGraphVertex vertex = graph.addVertex().getV();
        IBMGraphVertex vertex2 = graph.addVertex().getV();
        IBMGraphEdge edge = graph.addEdge(vertex, vertex2, "e1").getE();
        graph.commit();

        final String vertexId = vertex.getId().toString();
        final String vertex2Id = vertex2.getId().toString();
        final String edgeId = edge.getId().toString();


        Runnable r = new Runnable() {

            @Override
            public void run() {

                AtlasGraph<IBMGraphVertex,IBMGraphEdge> graph = getGraph();
                AtlasVertex<IBMGraphVertex,IBMGraphEdge> vertex = graph.getVertex(vertexId);
                AtlasVertex<IBMGraphVertex,IBMGraphEdge> vertex2 = graph.getVertex(vertex2Id);
                AtlasEdge<IBMGraphVertex,IBMGraphEdge> edge = graph.getEdge(edgeId);

                if(edge != null && edge.exists()) {
                    graph.removeEdge(edge);
                }

                if(vertex != null && vertex.exists()) {
                    graph.removeVertex(vertex);
                }

                if(vertex2 != null && vertex2.exists()) {
                    graph.removeVertex(vertex2);
                }
                //Make IBM Graph happy.  This forces the commits to be synchronized and results in
                //only one actual delete request being sent to IBM Graph.  Without this, every commit
                //except the first one fails due to locking errors coming from IBM Graph
                synchronized(TransactionMultiUserTest.class) {
                    graph.commit();
                }
                assertDeleted(vertex2);
                assertDeleted(graph.getVertex(vertexId));
                assertDeleted(graph.getVertex(vertex2Id));
                assertDeleted(graph.getEdge(edgeId));
            }

            private void assertDeleted(AtlasElement el) {
                assertTrue(el == null || ! el.exists());
            }

        };

        runInParallelInNewThreads(r, threadCount);
    }


    @Test
    public <V,E >void testSimultaneousElementsCreated() throws Throwable {

        //This test attempts to physically create many elements in the IBM
        //graph at the same time.  We do this by calling getId(), which forces
        //the elements to be physically created.  We then check to make sure that
        //the cache is intact and that no phantom vertices or edges were created.
        //
        //This test is designed to cause many simultaneous updates to the
        //Edge and Vertex caches maintained in IBMGraphGraph.  It ensures that
        //this does not cause the caches to become corrupt.

        final int threadCount = 20;
        Runnable r = new Runnable() {

            @Override
            public void run() {

                List<IBMGraphVertex> vertices = new ArrayList<>();
                AtlasGraph<IBMGraphVertex,IBMGraphEdge> graph = getGraph();
                for(int i = 0; i < 10; i++) {
                    vertices.add(graph.addVertex().getV());
                }
                List<IBMGraphEdge> edges = new ArrayList<>();
                for(int i = 0; i < vertices.size() - 2; i++) {
                    AtlasVertex<IBMGraphVertex,IBMGraphEdge> v1 = vertices.get(i);
                    AtlasVertex<IBMGraphVertex,IBMGraphEdge> v2 = vertices.get(i+1);
                    edges.add(graph.addEdge(v1, v2, "e" + i).getE());
                }

                //force creation of vertices and edges en masse
                for(IBMGraphVertex vertex : vertices) {
                    vertex.getId();
                }

                for(IBMGraphEdge edge : edges) {
                    edge.getId();
                }

                //with no synchronization, it's possible that some of the vertices did not end up being cached properly.  Exercise
                //the cache to try to flush out any bugs related to that
                for(IBMGraphVertex vertex : vertices) {
                    //check for phantoms by retrieving the vertex from the cache and then applying a change to it.
                    //That change should be reflected in the original vertex
                    IBMGraphVertex loaded = graph.getVertex(vertex.getId().toString()).getV();
                    vertex.setProperty("name", "Fred");
                    assertEquals("Fred", loaded.getProperty("name", String.class));

                    loaded.setProperty("age", "22");
                    assertEquals("22", vertex.getProperty("age", String.class));

                    assertTrue(vertex == loaded);
                }

                for(IBMGraphEdge edge : edges) {
                    //check for phantoms
                    IBMGraphEdge loaded = graph.getEdge(edge.getId().toString()).getE();
                    edge.setProperty("name", "Fred");
                    assertEquals("Fred", loaded.getProperty("name", String.class));

                    loaded.setProperty("age", "22");
                    assertEquals("22", edge.getProperty("age", String.class));

                    assertTrue(edge == loaded);
                }
            }
        };

        runInParallelInNewThreads(r, threadCount);


    }

    @Test
    public <V,E >void testSimultaneousLoadOfIncomingEdges() throws Throwable {

        //This test precreates a single vertices with a lot of incoming edges.
        //We then clear the cache.  Within each transaction, we load that vertex
        //and retrieve the list of incoming edges.  Each vertex maintains a list
        //of known incoming edges from the graph, and this test will cause many
        //simultaneous updates to that list to occur from different threads.  This
        //test ensures that the process of doing this does not corrupt the list.

        final int threadCount = 20;
        final int incomingEdgeCount = 100;

        AtlasGraph<IBMGraphVertex,IBMGraphEdge> graph = getGraph();
        IBMGraphVertex inVertex = graph.addVertex().getV();
        for(int i = 0; i < incomingEdgeCount; i++) {
            IBMGraphVertex outVertex = graph.addVertex().getV();
            graph.addEdge(outVertex, inVertex, "e" + i);
        }

        graph.commit();

        final String vertexId = inVertex.getId().toString();

        pushChangesAndFlushCache();

        Runnable r = new Runnable() {

            @Override
            public void run() {

                AtlasGraph<IBMGraphVertex,IBMGraphEdge> graph = getGraph();
                IBMGraphVertex inVertex = graph.getVertex(vertexId).getV();
                Iterable<AtlasEdge<IBMGraphVertex,IBMGraphEdge>> inEdges = inVertex.getEdges(AtlasEdgeDirection.IN);
                assertEquals(incomingEdgeCount, IteratorUtils.toList(inEdges.iterator()).size());

            }
        };

        runInParallelInNewThreads(r, threadCount);
    }

    @Test
    public <V,E >void testTransactionsAddOutgoingEdgesToSameVertex() throws Throwable {

        //This test pre-creates a single vertices.  Within each transaction, we load that vertex
        //and add 10 outgoing edges to the vertex, then commit the transaction.  We then verify that all
        //the edges are still there.  With all the threads doing this at the same time, this test
        //causes many simultaneous updates to the list of committed outgoing edges that is
        //maintained in the Vertex.  It verifies that these updates do not corrupt the list.

        final int threadCount = 20;
        final AtomicInteger edgeNum = new AtomicInteger();
        AtlasGraph<IBMGraphVertex,IBMGraphEdge> graph = getGraph();
        IBMGraphVertex vertex = graph.addVertex().getV();
        graph.commit();
        final String vertexId = vertex.getId().toString();

        Runnable r = new Runnable() {

            @Override
            public void run() {

                AtlasGraph<IBMGraphVertex,IBMGraphEdge> graph = getGraph();
                IBMGraphVertex outVertex = graph.getVertex(vertexId).getV();
                List<IBMGraphEdge> edgesAdded = new ArrayList<IBMGraphEdge>();
                for(int i = 0; i < 10; i++) {
                    IBMGraphVertex inVertex = graph.addVertex().getV();
                    IBMGraphEdge edge = graph.addEdge(outVertex, inVertex, "e" + edgeNum.incrementAndGet()).getE();
                    edgesAdded.add(edge);
                    //force creation of edge
                    edge.getId().toString();
                }
                graph.commit();

                for(IBMGraphEdge edge : edgesAdded) {
                    Iterator<AtlasEdge<IBMGraphVertex,IBMGraphEdge>> outEdges = outVertex.getEdges(AtlasEdgeDirection.OUT).iterator();
                    assertTrue(IteratorUtils.toList(outEdges).contains(edge));
                }
            }
        };

        runInParallelInNewThreads(r, threadCount);
    }

    @Test
    public <V,E >void testTransactionsAddIncomingEdgesToSameVertex() throws Throwable {

        //This test pre-creates a single vertices.  Within each transaction, we load that vertex
        //and add incoming 10 edges to the vertex, then commit the transaction.  We then verify that all
        //the edges are still there.  With all the threads doing this at the same time, this test
        //causes many simultaneous updates to the list of known committed incoming edges that is
        //maintained in the Vertex.  It verifies that these updates do not corrupt the list.

        final int threadCount = 20;
        final AtomicInteger edgeNum = new AtomicInteger(0);
        AtlasGraph<IBMGraphVertex,IBMGraphEdge> graph = getGraph();
        IBMGraphVertex vertex = graph.addVertex().getV();
        graph.commit();
        final String inVertexId = vertex.getId().toString();

        Runnable r = new Runnable() {

            @Override
            public void run() {

                AtlasGraph<IBMGraphVertex,IBMGraphEdge> graph = getGraph();
                IBMGraphVertex inVertex = graph.getVertex(inVertexId).getV();
                List<IBMGraphEdge> edgesAdded = new ArrayList<IBMGraphEdge>();
                for(int i = 0; i < 10; i++) {
                    IBMGraphVertex outVertex = graph.addVertex().getV();
                    IBMGraphEdge edge = graph.addEdge(outVertex, inVertex, "e" + edgeNum.incrementAndGet()).getE();
                    edgesAdded.add(edge);
                    //force creation of edge
                    edge.getId().toString();
                }
                graph.commit();

                for(IBMGraphEdge edge : edgesAdded) {
                    Iterator<AtlasEdge<IBMGraphVertex,IBMGraphEdge>> outEdges = inVertex.getEdges(AtlasEdgeDirection.IN).iterator();
                    assertTrue(IteratorUtils.toList(outEdges).contains(edge));
                }
            }
        };

        runInParallelInNewThreads(r, threadCount);
    }



    @Test
    public <V,E >void testTransactionsAddPropertiesToSameVertex() throws Throwable {

        //This test precreates and commits a bunch of vertices.  Within each transaction,
        //we then add a whole lot of distinct properties to the vertex.  The threads then
        //block, then when they all reach the same point, they all call commit() at the same
        //time to save push those new properties into IBM Graph.  We then verify that the property
        //is still set.  This test causes many simultaneous updates to list of committed properties stored
        //in each vertex.  Without proper synchronization, this test fails intermittently.  The very large
        //number of properties is needed to maximize the time each thread spends updating the list in the
        //Vertex, thus providing a bigger window for corruption to occur.  We can't make the list too big, though,
        //or the commit fails because the http request is too large.

        final int threadCount = 10;
        AtomicInteger counter = new AtomicInteger(0);
        AtlasGraph<IBMGraphVertex,IBMGraphEdge> graph = getGraph();
        IBMGraphVertex vertex = graph.addVertex().getV();
        graph.commit();
        final String vertexId = vertex.getId().toString();

        Runnable r = new Runnable() {

            @Override
            public void run() {
                boolean incremented = false;
                try {
                    final int propertiesToAdd = 50;
                    AtlasGraph<IBMGraphVertex,IBMGraphEdge> graph = getGraph();
                    IBMGraphVertex vertex = graph.getVertex(vertexId).getV();
                    for(int i = 0; i < propertiesToAdd; i++) {
                        vertex.setProperty(Thread.currentThread().getName() + "-" + i, "true");
                    }
                    counter.incrementAndGet();
                    incremented = true;
                    //wait for all threads to get here, so the commits happen at the same time
                    //(or as close as possible to at the same time)
                    while(counter.get() < threadCount) {
                        try {
                            Thread.sleep(1);
                        }
                        catch(InterruptedException e) {}
                    }
                    graph.commit();

                    for(int i = 0; i < propertiesToAdd; i++) {
                        assertEquals("true", vertex.getProperty(Thread.currentThread().getName() + "-" + i, String.class));
                    }
                }
                finally {
                    if(! incremented) {
                        //make sure that increment the integer in the event of failure so that the
                        //other threads do not hang waiting for all threads to increment the counter.
                        counter.incrementAndGet();
                    }
                }
            }
        };

        runInParallelInNewThreads(r, threadCount);
    }

    private List<String> getIds(List<? extends AtlasElement> elements) {
        return Lists.transform(elements, new Function<AtlasElement,String>() {

            @Override
            public String apply(AtlasElement input) {
                return input.getId().toString();
            }

        });
    }

}
