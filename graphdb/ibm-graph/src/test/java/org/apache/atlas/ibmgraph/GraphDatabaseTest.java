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

import com.google.common.collect.Iterables;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.ibmgraph.api.GraphDatabaseClient;
import org.apache.atlas.ibmgraph.api.json.JsonNewVertex;
import org.apache.atlas.ibmgraph.api.json.JsonVertex;
import org.apache.atlas.ibmgraph.http.HttpRequestDispatcher;
import org.apache.atlas.ibmgraph.http.HttpRequestHandler;
import org.apache.atlas.ibmgraph.http.IHttpRequestDispatcher;
import org.apache.atlas.ibmgraph.http.RequestType;
import org.apache.atlas.ibmgraph.util.Endpoint;
import org.apache.atlas.ibmgraph.util.GraphDBUtil;
import org.apache.atlas.repository.graphdb.AtlasCardinality;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery.Result;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.testng.annotations.Test;

import javax.script.ScriptException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.*;
/**
 *
 */
public class GraphDatabaseTest extends AbstractGraphDatabaseTest {

    @Test
    public void testElementIdListPropertyNotIndexed() {

        //This test validates the fix for OMS-572.

        IBMGraphGraph graph = getIbmGraphGraph();
        IBMGraphVertex v1 = graph.addVertex().getV();

        graph.commit();
        String v1Id = v1.getId().toString();
        //Flush cache, since creating v1 automatically adds it to indexer as part of setting the is_graph_db_vertex property.
        //For this test to work, v1.setProperty("name",...) must be the first call that reaches the indexer for v1.
        pushChangesAndFlushCache();

        v1 = graph.getVertex(v1Id).getV();
        IBMGraphVertex v2 = graph.addVertex().getV();

        IBMGraphEdge e1 = graph.addEdge(v1, v2, "knows").getE();
        IBMGraphEdge e2 = graph.addEdge(v1, v2, "has").getE();
        assertTrue(e1.isNewElement());
        v1.setPropertyFromElementsIds("edges", Arrays.asList(e1,e2));
        assertTrue(e1.isNewElement());
        v1.setProperty("name","Jeff");
        //prior to the changes for OMS-572, this assertion failed
        assertTrue(e1.isNewElement());

        graph.commit();
        pushChangesAndFlushCache();
        v1 = graph.getVertex(v1Id).getV();
        List<AtlasEdge> elements = v1.getListProperty("edges", AtlasEdge.class);
        assertEquals(e1, elements.get(0));
        assertEquals(e2, elements.get(1));
    }

    @Test
    public <V, E> void testResetHandler() {
        IHttpRequestDispatcher dispatcher = new HttpRequestDispatcher();
        HttpRequestHandler handler = new HttpRequestHandler(dispatcher);
        GraphDatabaseClient client = new GraphDatabaseClient(getIbmGraphGraph().getGraphId(), handler);

        JsonNewVertex v = new JsonNewVertex();
        JsonVertex v1 = createVertex(client, v);
        dispatcher.reset();
        JsonVertex v2 = createVertex(client, v);

        assertTrue(getGraph().getVertex(v1.getId()).exists());
        assertTrue(getGraph().getVertex(v2.getId()).exists());
    }

    public JsonVertex createVertex(GraphDatabaseClient client, JsonNewVertex vertex) {

        Gson gson = GraphDBUtil.getGson();
        String json = gson.toJson(vertex);
        JsonElement jsonResponse = client.processRequest(RequestType.POST, Endpoint.VERTICES.getUrl(getIbmGraphGraph().getGraphId()), json);
        return gson.fromJson(jsonResponse, JsonVertex.class);

    }

    @Test
    public <V, E> void testMultiplicityOnePropertySupport() {

        AtlasGraph<V, E> graph = (AtlasGraph<V, E>) getGraph();

        AtlasVertex<V, E> vertex = createVertex(graph);
        vertex.setProperty("name", "Jeff");
        vertex.setProperty("location", "Littleton");
        assertEquals("Jeff", vertex.getProperty("name", String.class));
        assertEquals("Littleton", vertex.getProperty("location", String.class));

        AtlasVertex<V, E> vertexCopy = graph.getVertex(vertex.getId().toString());

        assertEquals("Jeff", vertexCopy.getProperty("name", String.class));
        assertEquals("Littleton", vertexCopy.getProperty("location", String.class));

        assertTrue(vertexCopy.getPropertyKeys().contains("name"));
        assertTrue(vertexCopy.getPropertyKeys().contains("location"));

        assertTrue(vertexCopy.getPropertyValues("name", String.class).contains("Jeff"));
        assertTrue(vertexCopy.getPropertyValues("location", String.class).contains("Littleton"));
        assertTrue(vertexCopy.getPropertyValues("test", String.class).isEmpty());
        assertNull(vertexCopy.getProperty("test", String.class));

        pushChangesAndFlushCache();
        vertexCopy = graph.getVertex(vertex.getId().toString());

        assertEquals("Jeff", vertexCopy.getProperty("name", String.class));
        assertEquals("Littleton", vertexCopy.getProperty("location", String.class));

        assertTrue(vertexCopy.getPropertyKeys().contains("name"));
        assertTrue(vertexCopy.getPropertyKeys().contains("location"));

        assertTrue(vertexCopy.getPropertyValues("name", String.class).contains("Jeff"));
        assertTrue(vertexCopy.getPropertyValues("location", String.class).contains("Littleton"));
        assertTrue(vertexCopy.getPropertyValues("test", String.class).isEmpty());
        assertNull(vertexCopy.getProperty("test", String.class));

        //vertex is now stale, since we flushed the cache.  Graph
        //no longer knows about it
        vertex = vertexCopy;
        vertex.removeProperty("name");
        assertFalse(vertex.getPropertyKeys().contains("name"));
        assertNull(vertex.getProperty("name", String.class));
        assertTrue(vertex.getPropertyValues("name", String.class).isEmpty());

        vertexCopy = graph.getVertex(vertex.getId().toString());
        assertFalse(vertexCopy.getPropertyKeys().contains("name"));
        assertNull(vertexCopy.getProperty("name", String.class));
        assertTrue(vertexCopy.getPropertyValues("name", String.class).isEmpty());

        pushChangesAndFlushCache();
        vertexCopy = graph.getVertex(vertex.getId().toString());
        assertFalse(vertexCopy.getPropertyKeys().contains("name"));
        assertNull(vertexCopy.getProperty("name", String.class));
        assertTrue(vertexCopy.getPropertyValues("name", String.class).isEmpty());


    }

    @Test
    public <V,E> void testVertexUpdates() {

        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> v1 = createVertex(graph);


        AtlasVertex<V, E> v2 = createVertex(graph);
        v2.setProperty("age", "15");

        graph.addEdge(v2, v1, "knows");

        Iterable<AtlasVertex<V, E>> vertices = v1.query().direction(AtlasEdgeDirection.IN).vertices();
        AtlasVertex<V,E> loadedV2 = vertices.iterator().next();
        loadedV2.setProperty("age", "16");
        assertEquals(16, v2.getProperty("age", Integer.class).intValue());
    }

    @Test
    public <V,E> void testEdgeOperationsOnUnchangedVertex() {
        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> v1 = createVertex(graph);


        AtlasVertex<V, E> v2 = createVertex(graph);
        v2.setProperty("age", "15");

        AtlasEdge<V,E> edge = graph.addEdge(v2, v1, "knows");
        graph.commit();

        assertReferencesNothing(v1, AtlasEdgeDirection.OUT, "knows");
        assertReferences(v1, edge, AtlasEdgeDirection.IN, "knows");
        assertReferences(v1, edge, AtlasEdgeDirection.BOTH, "knows");

        assertReferencesNothing(v2, AtlasEdgeDirection.IN, "knows");
        assertReferences(v2, edge, AtlasEdgeDirection.OUT, "knows");
        assertReferences(v2, edge, AtlasEdgeDirection.BOTH, "knows");



    }

    private <V, E> void assertReferences(AtlasVertex<V, E> vertex, AtlasEdge<V, E> edge, AtlasEdgeDirection dir, String label) {
        assertEquals(1, Iterables.size(vertex.getEdges(dir)));
        assertEquals(edge, Iterables.get(vertex.getEdges(dir) ,0));
        if(label != null) {
            assertEquals(1, Iterables.size(vertex.getEdges(dir, label)));
            assertEquals(edge, Iterables.get(vertex.getEdges(dir, label) ,0));
        }
    }

    private <V, E> void assertReferencesNothing(AtlasVertex<V, E> vertex, AtlasEdgeDirection dir, String label) {
        assertEquals(0, Iterables.size(vertex.getEdges(dir)), 0);
        if(label != null) {
            assertEquals(0, Iterables.size(vertex.getEdges(dir, label)));

        }
    }

    @Test
    public <V,E> void testSpecialCharactersInPropertyValues() {
        AtlasGraph<V, E> graph = (AtlasGraph<V, E>) getGraph();

        AtlasVertex<V, E> vertex = createVertex(graph);
        String name = "\"Hello{}Wor'ld\"";
        String favoriteColor = "^hex[12345]";
        String age = "{}";
        String birthday = "1\\2\\3\\4\\5";
        String address = "^list[1,2,3]";

        vertex.setProperty("name", name);
        vertex.setProperty("age", age);
        vertex.setProperty("favoriteColor", favoriteColor);
        vertex.addProperty(TRAIT_NAMES, name);
        vertex.setProperty("birthday", birthday);
        vertex.setProperty("address", address);

        assertEquals(name, vertex.getProperty("name", String.class));
        assertEquals(age, vertex.getProperty("age", String.class));
        assertEquals(favoriteColor, vertex.getProperty("favoriteColor", String.class));
        assertEquals(birthday, vertex.getProperty("birthday", String.class));
        assertEquals(address, vertex.getProperty("address", String.class));
        assertTrue(vertex.getPropertyValues(TRAIT_NAMES, String.class).contains(name));
        assertTrue(vertex.getPropertyValues("name", String.class).contains(name));
        assertTrue(vertex.getPropertyValues("age", String.class).contains(age));
        assertTrue(vertex.getPropertyValues("favoriteColor", String.class).contains(favoriteColor));
        assertTrue(vertex.getPropertyValues("birthday", String.class).contains(birthday));
        assertTrue(vertex.getPropertyValues("address", String.class).contains(address));

        pushChangesAndFlushCache();

        AtlasVertex<V, E> vertexCopy = graph.getVertex(vertex.getId().toString());
        assertEquals(name, vertexCopy.getProperty("name", String.class));
        assertEquals(age, vertexCopy.getProperty("age", String.class));
        assertEquals(favoriteColor, vertexCopy.getProperty("favoriteColor", String.class));
        assertEquals(birthday, vertexCopy.getProperty("birthday", String.class));
        assertEquals(address, vertexCopy.getProperty("address", String.class));
        assertTrue(vertexCopy.getPropertyValues("name", String.class).contains(name));
        assertTrue(vertexCopy.getPropertyValues("age", String.class).contains(age));
        assertTrue(vertexCopy.getPropertyValues("favoriteColor", String.class).contains(favoriteColor));
        assertTrue(vertexCopy.getPropertyValues(TRAIT_NAMES, String.class).contains(name));
        assertTrue(vertexCopy.getPropertyValues("birthday", String.class).contains(birthday));
        assertTrue(vertexCopy.getPropertyValues("address", String.class).contains(address));
    }

    @Test
    public <V,E> void testEdgeProperty() {
        AtlasGraph<V,E> graph = (AtlasGraph<V,E>)getGraph();
        AtlasVertex<V,E> v1 = createVertex(graph);
        AtlasVertex<V,E> v2 = createVertex(graph);

        AtlasEdge<V,E> edge = graph.addEdge(v1, v2, "knows");

        edge.setProperty(WEIGHT_PROPERTY, "100");
        edge.setProperty("distance", "100 feet");

        assertEquals("100", edge.getProperty(WEIGHT_PROPERTY, String.class));
        assertEquals("100 feet", edge.getProperty("distance", String.class));

        AtlasEdge<V,E> edgeCopy = graph.getEdge(edge.getId().toString());
        assertEquals("100",edgeCopy.getProperty(WEIGHT_PROPERTY, String.class));
        assertEquals("100 feet", edgeCopy.getProperty("distance", String.class));

        pushChangesAndFlushCache();

        edgeCopy = graph.getEdge(edge.getId().toString());
        assertEquals(100, edgeCopy.getProperty(WEIGHT_PROPERTY, Integer.class).intValue());
        assertEquals("100 feet", edgeCopy.getProperty("distance", String.class));
    }

    @Test
    public <V,E> void testAddEdgeToCommittedVertex() {
        AtlasGraph<V,E> graph = (AtlasGraph<V,E>)getGraph();
        AtlasVertex<V,E> v1 = createVertex(graph);
        pushChangesAndFlushCache();
        AtlasVertex<V,E> v2 = createVertex(graph);
        pushChangesAndFlushCache();
        AtlasEdge<V,E> edge = graph.addEdge(v1, v2, "knows");
        pushChangesAndFlushCache();
        graph.addEdge(v1, v2, "likes");
        graph.addEdge(v2, v1, "likes");
        String edgeId = edge.getId().toString();

        pushChangesAndFlushCache();

        //make sure the edge exists
        AtlasEdge<V, E> edgeCopy = graph.getEdge(edgeId);
        assertNotNull(edgeCopy);
        assertEquals(edgeCopy, edge);


        graph.removeEdge(edgeCopy);

        edgeCopy = graph.getEdge(edge.getId().toString());
        //should return null now, since edge was deleted
        assertNull(edgeCopy);

        //force delete to be pushed to the graph
        pushChangesAndFlushCache();

        assertEdgeDeleted(edge.getId().toString());

    }

    @Test
    public <V,E> void testAddVertexProperty() {

        AtlasGraph<V,E> graph = (AtlasGraph<V,E>)getGraph();
        AtlasVertex<V,E> v1 = createVertex(graph);
        v1.setProperty("name", "Fred");

        assertEquals("Fred", v1.getProperty("name", String.class));

        graph.commit();

        assertEquals("Fred", v1.getProperty("name", String.class));

        String id = v1.getId().toString();
        pushChangesAndFlushCache();

        AtlasVertex<V,E> v1Copy = graph.getVertex(id);
        assertEquals("Fred", v1Copy.getProperty("name", String.class));

    }

    @Test
    public <V,E> void testAddEdgeScenarios() throws ScriptException, AtlasBaseException {

        AtlasGraph<V,E> graph = (AtlasGraph<V,E>)getGraph();
        graph.clear();

        // 1:  test add edge to uncommitted vertex
        AtlasVertex<V,E> v1 = createVertex(graph);
        v1.addProperty("name", "Fred");
        AtlasVertex<V,E> v2 = createVertex(graph);
        v2.addProperty("name", "Fred");
        AtlasEdge<V,E> e1 = graph.addEdge(v1, v2, "test1");
        graph.commit();

        //2:  test add edge to committed vertex w/ outgoing edge list
        AtlasEdge<V,E> e2 = graph.addEdge(v1, v2, "test2");
        graph.commit();

        pushChangesAndFlushCache();

        //3: test add edges to committed vertex loaded from the graph
        AtlasVertex<V,E> v1Loaded = graph.getVertex(v1.getId().toString());
        AtlasVertex<V,E> v2Loaded = graph.getVertex(v2.getId().toString());
        AtlasEdge<V,E> e3 = graph.addEdge(v1Loaded, v2Loaded,  "test3");

        //4:  test add edge to proxy vertex
        AtlasEdge<V,E> e4 = graph.getEdge(e2.getId().toString());
        graph.addEdge(e4.getInVertex(), e4.getOutVertex(), "test4");
        graph.commit();

        pushChangesAndFlushCache();

        //5:  test add edge to committed vertex w/o outgoing edge list
        List list = (List)graph.executeGremlinScript("g.V().has('name','Fred')", false);
        List<AtlasVertex<V,E>> convertedValues = list;
        assertEquals(2, convertedValues.size());
        AtlasEdge<V,E> e5 = graph.addEdge(convertedValues.get(0), convertedValues.get(1), "test5");
        graph.commit();

        pushChangesAndFlushCache();

        assertNotNull(graph.getEdge(e1.getId().toString()));
        assertNotNull(graph.getEdge(e2.getId().toString()));
        assertNotNull(graph.getEdge(e3.getId().toString()));
        assertNotNull(graph.getEdge(e4.getId().toString()));
        assertNotNull(graph.getEdge(e5.getId().toString()));

    }


    @Test
    public <V,E> void testRemoveEdgeScenarios() throws ScriptException, AtlasBaseException {

        AtlasGraph<V,E> graph = (AtlasGraph<V,E>)getGraph();
        graph.clear();

        // 1:  test add edge to uncommitted vertex
        AtlasVertex<V,E> v1 = createVertex(graph);
        v1.addProperty("name", "Fred");
        AtlasVertex<V,E> v2 = createVertex(graph);
        v2.addProperty("name", "Fred");
        AtlasEdge<V,E> e1 = graph.addEdge(v1, v2, "test1");
        AtlasEdge<V,E> e2 = graph.addEdge(v1, v2, "test2");
        AtlasEdge<V,E> e3 = graph.addEdge(v1, v2, "test3");
        AtlasEdge<V,E> e4 = graph.addEdge(v1, v2, "test4");
        AtlasEdge<V,E> e5 = graph.addEdge(v1, v2, "test5");

        graph.removeEdge(e1);
        graph.commit();

        String e2Id = e2.getId().toString();
        //2:  test add edge to committed vertex w/ outgoing edge list
        graph.removeEdge(e2);
        graph.commit();

        pushChangesAndFlushCache();
        //3: test remove edges to committed vertex loaded from the graph

        //load/cache v1 and v2
        AtlasVertex<V,E> v1Loaded = graph.getVertex(v1.getId().toString());
        AtlasVertex<V,E> v2Loaded = graph.getVertex(v2.getId().toString());

        //force proxy resolution
        v1Loaded.getProperty("name", String.class);
        v2Loaded.getProperty("name", String.class);
        String e3Id = e3.getId().toString();
        AtlasEdge<V,E> e3Loaded = graph.getEdge(e3Id);
        graph.removeEdge(e3Loaded);
        graph.commit();

        //3:  test remove edge from proxy vertex
        String e4Id = e4.getId().toString();
        AtlasEdge<V,E> e4Copy = graph.getEdge(e4Id);
        graph.removeEdge(e4Copy);
        graph.commit();

        pushChangesAndFlushCache();

        //4:  test add edge to committed vertex w/o outgoing edge list
        List list = (List)graph.executeGremlinScript("g.V().has('name','Fred')", false);

        String e5Id = e5.getId().toString();
        AtlasEdge<V,E> e5Copy = graph.getEdge(e5Id);
        graph.removeEdge(e5Copy);
        graph.commit();

        pushChangesAndFlushCache();

        assertEdgeDeleted(e2Id);
        assertEdgeDeleted(e3Id);
        assertEdgeDeleted(e4Id);
        assertEdgeDeleted(e5Id);

    }

    private <V,E> void assertEdgeDeleted(String id) {
        AtlasGraph<V,E> graph = (AtlasGraph<V,E>)getGraph();
        AtlasEdge<V,E> edge = graph.getEdge(id);
        if(edge == null) {
            return;
        }
        assertFalse(edge.exists());
    }

    @Test
    public <V,E> void testRemoveUncommittedEdge() {

        AtlasGraph<V,E> graph = (AtlasGraph<V,E>)getGraph();
        AtlasVertex<V,E> v1 = createVertex(graph);
        AtlasVertex<V,E> v2 = createVertex(graph);

        AtlasEdge<V,E> edge = graph.addEdge(v1, v2, "knows");

        //make sure the edge exists
        String edgeId = edge.getId().toString();
        AtlasEdge<V, E> edgeCopy = graph.getEdge(edgeId);
        assertNotNull(edgeCopy);
        assertEquals(edgeCopy, edge);


        graph.removeEdge(edge);

        edgeCopy = graph.getEdge(edgeId);
        //should return null now, since edge was deleted
        assertNull(edgeCopy);

        //force delete to be pushed to the graph
        pushChangesAndFlushCache();

        assertEdgeDeleted(edgeId);
    }

    @Test
    public <V,E> void testRemoveCommittedEdge() {

        AtlasGraph<V,E> graph = (AtlasGraph<V,E>)getGraph();
        AtlasVertex<V,E> v1 = createVertex(graph);
        AtlasVertex<V,E> v2 = createVertex(graph);

        AtlasEdge<V,E> edge = graph.addEdge(v1, v2, "knows");

        pushChangesAndFlushCache();

        //make sure the edge exists
        AtlasEdge<V, E> edgeCopy = graph.getEdge(edge.getId().toString());
        assertNotNull(edgeCopy);
        assertEquals(edgeCopy, edge);


        graph.removeEdge(edgeCopy);

        edgeCopy = graph.getEdge(edge.getId().toString());
        //should return null now, since edge was deleted
        assertNull(edgeCopy);

        //force delete to be pushed to the graph
        pushChangesAndFlushCache();

        assertEdgeDeleted(edge.getId().toString());
    }

    @Test
    public <V,E> void testRemoveVertex() {

        AtlasGraph<V,E> graph = (AtlasGraph<V,E>)getGraph();

        AtlasVertex<V,E> v1 = createVertex(graph);

        String v1Id = v1.getId().toString();
        assertNotNull(graph.getVertex(v1Id));

        graph.removeVertex(v1);

        assertNull(graph.getVertex(v1Id));

        //force delete to be pushed to the graph
        pushChangesAndFlushCache();

        assertFalse(graph.getVertex(v1Id).exists());
    }

    @Test
    public <V,E> void testRemoveVertexWithAssociatedEdges() {

        AtlasGraph<V,E> graph = (AtlasGraph<V,E>)getGraph();

        AtlasVertex<V,E> v1 = createVertex(graph);
        AtlasVertex<V,E> v2 = createVertex(graph);
        AtlasVertex<V,E> v3 = createVertex(graph);

        graph.addEdge(v1, v2, "outEdge");
        graph.addEdge(v3, v1, "inEdge");
        String v1Id = v1.getId().toString();
        assertNotNull(graph.getVertex(v1Id));
        pushChangesAndFlushCache();
        AtlasVertex<V,E> toRemove = graph.getVertex(v1Id);
        graph.removeVertex(toRemove);

        assertNull(graph.getVertex(v1Id));

        //force delete to be pushed to the graph
        pushChangesAndFlushCache();

        assertFalse(graph.getVertex(v1Id).exists());
    }



    @Test
    public <V,E> void testRemoveNonExistentVertex() {

        AtlasGraph<V,E> graph = (AtlasGraph<V,E>)getGraph();

        AtlasVertex<V,E> v1 = createVertex(graph);

        String v1Id = v1.getId().toString();
        assertNotNull(graph.getVertex(v1Id));

        graph.removeVertex(v1);
        graph.removeVertex(v1); //should be no-op

        assertNull(graph.getVertex(v1Id));
    }

    @Test
    public <V,E> void testGetEdges() {


        AtlasGraph<V,E> graph = getGraph();
        AtlasVertex<V,E> v1 = createVertex(graph);
        AtlasVertex<V,E> v2 = createVertex(graph);
        AtlasVertex<V,E> v3 = createVertex(graph);

        AtlasEdge<V,E> knows = graph.addEdge(v2, v1, "knows");
        AtlasEdge<V,E> eats =  graph.addEdge(v3, v1, "eats");
        AtlasEdge<V,E> drives = graph.addEdge(v3, v2, "drives");
        AtlasEdge<V,E> sleeps = graph.addEdge(v2, v3, "sleeps");


        {
            List<AtlasEdge<V,E>> edges =  IteratorUtils.asList(v1.getEdges(AtlasEdgeDirection.IN));
            assertEquals(2, edges.size());
            assertTrue(edges.contains(knows));
            assertTrue(edges.contains(eats));
        }

        {
            List<AtlasEdge<V,E>> edges =  IteratorUtils.asList(v1.getEdges(AtlasEdgeDirection.OUT));
            assertTrue(edges.isEmpty());
        }

        {
            List<AtlasEdge<V,E>> edges =  IteratorUtils.asList(v1.getEdges(AtlasEdgeDirection.BOTH));
            assertEquals(2, edges.size());
            assertTrue(edges.contains(knows));
            assertTrue(edges.contains(eats));
        }

        {
            List<AtlasEdge<V,E>> edges =  IteratorUtils.asList(v1.getEdges(AtlasEdgeDirection.IN, "knows"));
            assertEquals(1, edges.size());
            assertTrue(edges.contains(knows));
        }

        {
            List<AtlasEdge<V,E>> edges =  IteratorUtils.asList(v1.getEdges(AtlasEdgeDirection.BOTH, "knows"));
            assertEquals(1, edges.size());
            assertTrue(edges.contains(knows));
        }

        {
            List<AtlasEdge<V,E>> edges =  IteratorUtils.asList(v2.getEdges(AtlasEdgeDirection.IN));
            assertEquals(1, edges.size());
            assertTrue(edges.contains(drives));
        }


        {
            List<AtlasEdge<V,E>> edges =  IteratorUtils.asList(v2.getEdges(AtlasEdgeDirection.OUT));
            assertEquals(2, edges.size());
            assertTrue(edges.contains(knows));
            assertTrue(edges.contains(sleeps));
        }

        {
            List<AtlasEdge<V,E>> edges =  IteratorUtils.asList(v2.getEdges(AtlasEdgeDirection.BOTH));
            assertEquals(3, edges.size());
            assertTrue(edges.contains(knows));
            assertTrue(edges.contains(sleeps));
            assertTrue(edges.contains(drives));
        }

        {
            List<AtlasEdge<V,E>> edges =  IteratorUtils.asList(v2.getEdges(AtlasEdgeDirection.BOTH,"delivers"));
            assertEquals(0, edges.size());
        }


    }

    @Test
    public <V,E> void testListProperties() throws AtlasException {
        AtlasGraph<V,E> graph = getGraph();
        AtlasVertex<V,E> vertex = createVertex(graph);
        vertex.setListProperty("colors", Arrays.asList(new String[]{"red","blue","green"}));
        List<String> colors = vertex.getListProperty("colors");
        assertTrue(colors.contains("red"));
        assertTrue(colors.contains("blue"));
        assertTrue(colors.contains("green"));

        AtlasVertex<V,E> vertexCopy = graph.getVertex(vertex.getId().toString());
        colors = vertexCopy.getListProperty("colors");
        assertTrue(colors.contains("red"));
        assertTrue(colors.contains("blue"));
        assertTrue(colors.contains("green"));

        colors = vertexCopy.getProperty("colors", List.class);
        assertTrue(colors.contains("red"));
        assertTrue(colors.contains("blue"));
        assertTrue(colors.contains("green"));

        pushChangesAndFlushCache();
        vertexCopy = graph.getVertex(vertex.getId().toString());
        colors = vertexCopy.getListProperty("colors");
        assertTrue(colors.contains("red"));
        assertTrue(colors.contains("blue"));
        assertTrue(colors.contains("green"));

        colors = vertexCopy.getProperty("colors", List.class);
        assertTrue(colors.contains("red"));
        assertTrue(colors.contains("blue"));
        assertTrue(colors.contains("green"));

    }

    @Test
    public <V,E> void testPropertyDataTypes() {


        //primitives
        AtlasGraph<V,E> graph = getGraph();

        testProperty(graph, "booleanProperty", Boolean.TRUE);
        testProperty(graph, "booleanProperty", Boolean.FALSE);
        testProperty(graph, "booleanProperty", new Boolean(Boolean.TRUE));
        testProperty(graph, "booleanProperty", new Boolean(Boolean.FALSE));


        testProperty(graph, "byteProperty",  Byte.MAX_VALUE);
        testProperty(graph, "byteProperty", Byte.MIN_VALUE);
        testProperty(graph, "byteProperty",  new Byte(Byte.MAX_VALUE));
        testProperty(graph, "byteProperty", new Byte(Byte.MIN_VALUE));



        testProperty(graph, "shortProperty",  Short.MAX_VALUE);
        testProperty(graph, "shortProperty", Short.MIN_VALUE);
        testProperty(graph, "shortProperty",  new Short(Short.MAX_VALUE));
        testProperty(graph, "shortProperty", new Short(Short.MIN_VALUE));

        testProperty(graph, "intProperty", Integer.MAX_VALUE);
        testProperty(graph, "intProperty", Integer.MIN_VALUE);
        testProperty(graph, "intProperty", new Integer(Integer.MAX_VALUE));
        testProperty(graph, "intProperty", new Integer(Integer.MIN_VALUE));

        testProperty(graph, "longProperty", Long.MIN_VALUE);
        testProperty(graph, "longProperty", Long.MAX_VALUE);
        testProperty(graph, "longProperty", new Long(Long.MIN_VALUE));
        testProperty(graph, "longProperty", new Long(Long.MAX_VALUE));


        testProperty(graph, "doubleProperty", Double.MAX_VALUE);
        testProperty(graph, "doubleProperty", Double.MIN_VALUE);
        testProperty(graph, "doubleProperty", new Double(Double.MAX_VALUE));
        testProperty(graph, "doubleProperty", new Double(Double.MIN_VALUE));

        testProperty(graph, "floatProperty", Float.MAX_VALUE);
        testProperty(graph, "floatProperty", Float.MIN_VALUE);
        testProperty(graph, "floatProperty", new Float(Float.MAX_VALUE));
        testProperty(graph, "floatProperty", new Float(Float.MIN_VALUE));

        //enumerations - TypeCategory
        testProperty(graph, "typeCategoryProperty", TypeCategory.CLASS);

        //biginteger
        testProperty(graph, "bigIntegerProperty", new BigInteger(String.valueOf(Long.MAX_VALUE)).multiply(BigInteger.TEN));

        //bigdecimal
        BigDecimal bigDecimal = new BigDecimal(Double.MAX_VALUE);
        testProperty(graph, "bigDecimalProperty", bigDecimal.multiply(bigDecimal));


    }

    private <V,E> void testProperty(AtlasGraph<V,E> graph, String name, Object value) {

        //Atlas will take care of converting the value that comes back to the right
        //type.  We just need to make sure that the property values are storable.
        AtlasVertex<V,E> vertex = createVertex(graph);
        vertex.setProperty(name, value);

        assertNotNull(vertex.getProperty(name, String.class));
        AtlasVertex<V,E> loaded = graph.getVertex(vertex.getId().toString());
        assertEquals(value, loaded.getProperty(name, value.getClass()));

        pushChangesAndFlushCache();
        loaded = graph.getVertex(vertex.getId().toString());
        assertEquals(value, loaded.getProperty(name, value.getClass()));

    }

    @Test
    public <V,E >void testMultiPropertySupport() {

        AtlasGraph<V,E> graph = getGraph();
        String vertexId;

        AtlasVertex<V, E> vertex = createVertex(graph);
        vertexId = vertex.getId().toString();
        vertex.setProperty(TRAIT_NAMES, "trait1");
        vertex.setProperty(TRAIT_NAMES, "trait2");
        assertEquals(2, vertex.getPropertyValues(TRAIT_NAMES, String.class).size());
        vertex.addProperty(TRAIT_NAMES, "trait3");
        vertex.addProperty(TRAIT_NAMES, "trait4");

        assertTraitsPresent(vertex);

        assertTraitsPresent(graph.getVertex(vertexId));

        pushChangesAndFlushCache();
        assertTraitsPresent(graph.getVertex(vertexId));
    }

    @Test
    public <V,E >void testClearMultiProperty() {

        AtlasGraph<V,E> graph = getGraph();
        String vertexId;

        AtlasVertex<V, E> vertex = createVertex(graph);

        vertex.setProperty(TRAIT_NAMES, "trait1");
        vertex.setProperty(TRAIT_NAMES, "trait2");
        graph.commit();
        vertexId = vertex.getId().toString();
        vertex.removeProperty(TRAIT_NAMES);
        vertex.setProperty(TRAIT_NAMES, "trait1");
        vertex.setProperty(TRAIT_NAMES, "trait2");
        vertex.setProperty(TRAIT_NAMES, "trait3");
        vertex.setProperty(TRAIT_NAMES, "trait4");

        assertTraitsPresent(vertex);

        assertTraitsPresent(graph.getVertex(vertexId));

        pushChangesAndFlushCache();
        assertTraitsPresent(graph.getVertex(vertexId));
    }

    @Test
    public void testMultiPropertyDuplicateHandling() {

        AtlasGraph graph = getGraph();
        String vertexId;

        AtlasVertex vertex = createVertex(graph);
        //no clear, value not previously present
        vertex.setProperty(TRAIT_NAMES, "trait1");
        vertex.setProperty(TRAIT_NAMES, "trait1");
        assertHasPropertyValues(vertex, TRAIT_NAMES, "trait1");

        //test add trait value that is already present
        vertex = getGraph().getVertex(vertex.getId().toString());
        vertex.setProperty(TRAIT_NAMES, "trait1");
        assertHasPropertyValues(vertex, TRAIT_NAMES, "trait1");

        //test add trait value back after clear
        vertex = getGraph().getVertex(vertex.getId().toString());
        vertex.removeProperty(TRAIT_NAMES);
        vertex.setProperty(TRAIT_NAMES, "trait1");
        assertHasPropertyValues(vertex, TRAIT_NAMES, "trait1");

        //test add new duplicate trait after a clear
        vertex = getGraph().getVertex(vertex.getId().toString());
        vertex.removeProperty(TRAIT_NAMES);
        vertex.setProperty(TRAIT_NAMES, "trait2");
        vertex.setProperty(TRAIT_NAMES, "trait2");
        assertHasPropertyValues(vertex, TRAIT_NAMES, "trait2");

    }

    private void assertHasPropertyValues(AtlasVertex vertex, String key, String...expectedValues) {
        Collection<String> foundValues = vertex.getPropertyValues(key, String.class);
        assertEquals(expectedValues.length, foundValues.size());
        for(String expectedValue : expectedValues) {
            assertTrue(foundValues.contains(expectedValue));
        }
        pushChangesAndFlushCache();

        AtlasVertex vertexCopy = getGraph().getVertex(vertex.getId().toString());
        foundValues = vertexCopy.getPropertyValues(key, String.class);
        assertEquals(expectedValues.length, foundValues.size());
        for(String expectedValue : expectedValues) {
            assertTrue(foundValues.contains(expectedValue));
        }
    }




    private <V, E> void assertTraitsPresent(AtlasVertex<V, E> vertexCopy) {
        assertTrue(vertexCopy.getPropertyKeys().contains(TRAIT_NAMES));
        Collection<String> traitNames = vertexCopy.getPropertyValues(TRAIT_NAMES, String.class);
        assertTrue(traitNames.contains("trait1"));
        assertTrue(traitNames.contains("trait2"));
        assertTrue(traitNames.contains("trait3"));
        assertTrue(traitNames.contains("trait4"));

        try {
            vertexCopy.getProperty(TRAIT_NAMES, String.class);
        }
        catch(IllegalStateException expected) {
            //multiple property values exist
        }

    }

    @Test
    public <V,E> void testRemoveProperty() {

        AtlasGraph<V,E> graph = getGraph();
        AtlasVertex<V, E> vertex = createVertex(graph);
        vertex.setProperty(TRAIT_NAMES, "trait1");
        vertex.setProperty(TRAIT_NAMES, "trait1");
        vertex.setProperty("name", "Jeff");


        //remove non-existing property - multiplicity one
        vertex.removeProperty("jeff");

        assertFalse(vertex.getPropertyKeys().contains("jeff"));

        vertex.removeProperty("name");
        assertFalse(vertex.getPropertyKeys().contains("name"));

        //remove existing property - multiplicity many
        vertex.removeProperty(TRAIT_NAMES);
        assertFalse(vertex.getPropertyKeys().contains(TRAIT_NAMES));

        AtlasVertex<V, E> vertexCopy = graph.getVertex(vertex.getId().toString());
        assertFalse(vertexCopy.getPropertyKeys().contains("jeff"));
        assertFalse(vertexCopy.getPropertyKeys().contains(TRAIT_NAMES));

        //remove non-existing property
        vertex.removeProperty(TRAIT_NAMES);
        vertex.removeProperty("jeff");

        pushChangesAndFlushCache();

        vertexCopy = graph.getVertex(vertex.getId().toString());
        assertFalse(vertexCopy.getPropertyKeys().contains("jeff"));
        assertFalse(vertexCopy.getPropertyKeys().contains(TRAIT_NAMES));

        vertex.removeProperty(TRAIT_NAMES);
        vertex.removeProperty("jeff");
    }



    @Test
    public <V,E >void testPersistList() throws AtlasException {
        AtlasGraph<V,E> graph = getGraph();
        AtlasVertex<V, E> vertex = createVertex(graph);
        List<String> value = Arrays.asList(new String[]{"joe,","bob\\\"", "Mike"});
        vertex.setProperty("name", value);
        vertex.setProperty("emptyList", Collections.emptyList());

        assertListsEqual(value, (List)vertex.getProperty("name", Object.class));
        assertListsEqual(value, (List)vertex.getListProperty("name"));
        assertListsEqual(Collections.emptyList(), (List)vertex.getProperty("emptyList", Object.class));
        assertListsEqual(Collections.emptyList(), (List)vertex.getListProperty("emptyList"));
        pushChangesAndFlushCache();

        AtlasVertex vertexCopy = graph.getVertex(vertex.getId().toString());
        assertListsEqual(value, (List)vertexCopy.getProperty("name", Object.class));
        assertListsEqual(value, (List)vertexCopy.getListProperty("name"));
        assertListsEqual(Collections.emptyList(), (List)vertex.getProperty("emptyList", Object.class));
        assertListsEqual(Collections.emptyList(), (List)vertex.getListProperty("emptyList"));

    }

    private void assertListsEqual(List<String> value, List<String> found) {
        assertEquals(value.size(), found.size());
        for(int i = 0; i < value.size(); i++) {
            assertEquals(value.get(i), found.get(i));
        }
    }
    @Test
    public <V,E >void testAddMultManyPropertyValueTwice() {

        AtlasGraph<V,E> graph = getGraph();
        String vertexId;
        {
            AtlasVertex<V, E> vertex = createVertex(graph);
            vertexId = vertex.getId().toString();
            vertex.setProperty(TRAIT_NAMES, "trait1");
            vertex.setProperty(TRAIT_NAMES, "trait1");
            vertex.addProperty(TRAIT_NAMES, "trait2");
            vertex.addProperty(TRAIT_NAMES, "trait2");
            assertEquals(2, vertex.getPropertyValues(TRAIT_NAMES, String.class).size());
            assertTrue(vertex.getPropertyKeys().contains(TRAIT_NAMES));
            Collection<String> traitNames = vertex.getPropertyValues(TRAIT_NAMES, String.class);
            assertTrue(traitNames.contains("trait1"));
            assertTrue(traitNames.contains("trait2"));

        }

        {
            AtlasVertex<V,E> vertexCopy = graph.getVertex(vertexId);
            assertTrue(vertexCopy.getPropertyKeys().contains(TRAIT_NAMES));
            Collection<String> traitNames = vertexCopy.getPropertyValues(TRAIT_NAMES, String.class);
            assertTrue(traitNames.contains("trait1"));
            assertTrue(traitNames.contains("trait2"));
        }

        pushChangesAndFlushCache();

        {
            AtlasVertex<V,E> vertexCopy = graph.getVertex(vertexId);
            assertTrue(vertexCopy.getPropertyKeys().contains(TRAIT_NAMES));
            Collection<String> traitNames = vertexCopy.getPropertyValues(TRAIT_NAMES, String.class);
            assertTrue(traitNames.contains("trait1"));
            assertTrue(traitNames.contains("trait2"));
        }
    }




    private <V, E> void assertResultContainsVertex(Iterator<Result<V, E>> vertices, String vertexId) {
        boolean vertexFound = false;
        while (vertices.hasNext()) {
            AtlasVertex v = vertices.next().getVertex();
            if (v.getId() == vertexId) {
                vertexFound = true;
                break;
            }
        }
        assertTrue(vertexFound);
    }

    @Test
    public <V, E>void testJsonSerialization() throws JSONException {
        AtlasGraph<V, E> graph = getGraph();
        //create __type index.
        AtlasVertex v1 = (AtlasVertex) graph.addVertex();
        v1.setProperty(typeProperty, "typeSystem-test2");
        v1.setJsonProperty("properties", new AttributeInfo("test-attribute", "Class", new  MultiplicityTest(0, 0, false), false, false, false).toJson());
        AtlasGraphQuery<V, E> query = graph.query();
        //query it and parse the response
        query.has(typeProperty, "typeSystem-test2");
        Iterable<? extends AtlasVertex<V, E>> result = query.vertices();
        List<IBMGraphVertex> list = IteratorUtils.asList(result);


        for (IBMGraphVertex vertex : list) {

            String props = vertex.getJsonProperty("properties");
            if (props != null) {
                try {
                    System.out.println(" got value " + props);
                    AttributeInfo attributeJson = fromJsonAttribute(props);
                    assertNotNull(attributeJson);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    @Test
    public <V,E> void testElementHashingWithIdChange() {

        AtlasGraph<V, E> graph = getGraph();
        Map<AtlasElement, Integer> map = new HashMap<>();
        AtlasVertex<V, E> v1 = createVertex(graph);
        AtlasVertex<V, E> v2 = createVertex(graph);
        AtlasVertex<V, E> v3 = createVertex(graph);
        map.put(v1, 1);
        map.put(v2, 2);
        map.put(v3, 3);
        assertEquals(1, map.get(v1).intValue());
        assertEquals(2, map.get(v2).intValue());
        assertEquals(3, map.get(v3).intValue());
        graph.commit();
        assertEquals(1, map.get(v1).intValue());
        assertEquals(2, map.get(v2).intValue());
        assertEquals(3, map.get(v3).intValue());


    }



    private final class MultiplicityTest {

        public final int lower;
        public final int upper;
        public final boolean isUnique;

        public MultiplicityTest(int lower, int upper, boolean isUnique) {
            assert lower >= 0;
            assert upper >= 0;
            assert upper >= lower;
            this.lower = lower;
            this.upper = upper;
            this.isUnique = isUnique;
        }

        public String toJson() throws JSONException {
            JSONObject json = new JSONObject(false, null, true, false);
            json.put("lower", lower);
            json.put("upper", upper);
            json.put("isUnique", isUnique);

            return json.toString();
        }
    }

    private class AttributeInfo {
        private String name = "test";
        private MultiplicityTest multiplicity;
        private boolean isComposite = false;
        private boolean isUnique = false;
        private boolean isIndexable = false;


        public AttributeInfo(String name, String datatype,
                MultiplicityTest multiplicity, boolean isComposite,
                boolean isUnique, boolean isIndexable) {
            super();
            this.name = name;
            this.multiplicity = multiplicity;
            this.isComposite = isComposite;
            this.isUnique = isUnique;
            this.isIndexable = isIndexable;
        }

        /**
         * If this is a reference attribute, then the name of the attribute on
         * the Class that this refers to.
         */
        public String toJson() throws JSONException {
            JSONObject json = new JSONObject(false, null, true, false);
            json.put("name", name);
            json.put("multiplicity", multiplicity.toJson());
            json.put("isComposite", isComposite);
            json.put("isUnique", isUnique);
            json.put("isIndexable", isIndexable);
            json.put("dataType", "Class");
            return json.toString();
        }

    }

    private static AttributeInfo fromJsonAttribute(String jsonStr)
            throws JSONException {
        GraphDatabaseTest test = new GraphDatabaseTest();
        JSONObject json = new JSONObject(jsonStr);
        return test.new AttributeInfo(json.getString("name"),
                json.getString("dataType"), test.fromJson(json.getString("multiplicity")),
                json.getBoolean("isComposite"),
                json.getBoolean("isUnique"),
                json.getBoolean("isIndexable"));
    }

    private  MultiplicityTest fromJson(String jsonStr) throws JSONException {
        JSONObject json = new JSONObject(jsonStr);
        return new MultiplicityTest(json.getInt("lower"), json.getInt("upper"),
                json.getBoolean("isUnique"));
    }

    @Test
    public <V,E >void testSetElementIdListPropertyWhereElementCreatedInGremlinScript() throws AtlasException {

        AtlasGraph<V, E> graph = getGraph();
        new HashMap<>();
        AtlasVertex<V, E> v1 = createVertex(graph);
        AtlasVertex<V, E> v2 = createVertex(graph);

        AtlasEdge<V,E> e1 = graph.addEdge(v1, v2, "e1");
        graph.addEdge(v1, v2, "e2");
        AtlasEdge<V,E> e3 = graph.addEdge(v1, v2, "e3");
        graph.addEdge(v1, v2, "e4");

        v1.setPropertyFromElementsIds("oddEdges", Arrays.asList(e1, e3));
        List<AtlasEdge> oddEdges = v1.getListProperty("oddEdges", AtlasEdge.class);
        assertTrue(oddEdges.contains(e1));
        assertTrue(oddEdges.contains(e3));
        assertFalse(e1.isIdAssigned());
        assertFalse(e3.isIdAssigned());
        graph.commit();
        validOddEdgesPropertyValue(v1, e1, e3);

        pushChangesAndFlushCache();

        AtlasVertex<V,E> v1Loaded = graph.getVertex(v1.getId().toString());
        validOddEdgesPropertyValue(v1Loaded, e1, e3);

    }

    @Test
    public <V,E >void testSetElementIdListPropertyWhereElementNotCreatedInGremlinScript() throws AtlasException {

        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> v1 = createVertex(graph);
        AtlasVertex<V, E> v2 = createVertex(graph);

        AtlasEdge<V,E> e1 = graph.addEdge(v1, v2, "e1");
        graph.addEdge(v1, v2, "e2");
        AtlasEdge<V,E> e3 = graph.addEdge(v1, v2, "e3");
        graph.addEdge(v1, v2, "e4");

        v1.setPropertyFromElementsIds("oddEdges", Arrays.asList(e1, e3));

        //here, we are explicitly requesting the ids of edges, forcing them to be created
        //prior to the commit
        validOddEdgesPropertyValue(v1, e1, e3);
        assertTrue(e1.isIdAssigned());
        assertTrue(e3.isIdAssigned());
        graph.commit();
        validOddEdgesPropertyValue(v1, e1, e3);

        pushChangesAndFlushCache();

        AtlasVertex<V,E> v1Loaded = graph.getVertex(v1.getId().toString());
        validOddEdgesPropertyValue(v1Loaded, e1, e3);

    }

    private <V, E> void validOddEdgesPropertyValue(AtlasVertex<V, E> v1, AtlasEdge<V, E> e1, AtlasEdge<V, E> e2)
            throws AtlasException {
        List<String> values = v1.getListProperty("oddEdges");
        assertEquals(2, values.size());
        assertTrue(values.contains(e1.getId().toString()));
        assertTrue(values.contains(e2.getId().toString()));

        List<AtlasEdge> edgeValues = v1.getListProperty("oddEdges", AtlasEdge.class);
        assertEquals(2, values.size());
        assertTrue(edgeValues.contains(e1));
        assertTrue(edgeValues.contains(e2));
    }

    @Test
    public <V,E >void testSetElementIdPropertyWhereElementCreatedInGremlinScript() throws AtlasException {

        AtlasGraph<V, E> graph = getGraph();
        new HashMap<>();
        AtlasVertex<V, E> v1 = createVertex(graph);
        AtlasVertex<V, E> v2 = createVertex(graph);

        AtlasEdge<V,E> e1 = graph.addEdge(v1, v2, "e1Label");
        AtlasEdge<V,E> e2 = graph.addEdge(v1, v2, "e2Label");
        v1.setPropertyFromElementId("edgeOne", e1);
        v1.setPropertyFromElementId("edgeTwo", e2);
        assertEquals(e1, v1.getProperty("edgeOne", AtlasEdge.class));
        assertEquals(e2, v1.getProperty("edgeTwo", AtlasEdge.class));

        assertFalse(e1.isIdAssigned());
        assertFalse(e2.isIdAssigned());
        assertFalse(e1.isIdAssigned());
        assertFalse(e2.isIdAssigned());
        graph.commit();
        validateEdgeProperties(v1, e1, e2);

        pushChangesAndFlushCache();

        AtlasVertex<V,E> v1Loaded = graph.getVertex(v1.getId().toString());
        validateEdgeProperties(v1Loaded, e1, e2);
    }

    @Test
    public <V,E >void testElementIdPropertyWhereNotElementCreatedInGremlinScript() throws AtlasException {

        AtlasGraph<V, E> graph = getGraph();

        AtlasVertex<V, E> v1 = createVertex(graph);
        AtlasVertex<V, E> v2 = createVertex(graph);

        AtlasEdge<V,E> e1 = graph.addEdge(v1, v2, "e1");
        AtlasEdge<V,E> e2 = graph.addEdge(v1, v2, "e2");
        v1.setPropertyFromElementId("edgeOne", e1);
        v1.setPropertyFromElementId("edgeTwo", e2);


        //here, we are explicitly requesting the ids of edges, forcing them to be created
        //prior to the commit
        validateEdgeProperties(v1, e1, e2);
        assertTrue(e1.isIdAssigned());
        assertTrue(e2.isIdAssigned());
        assertTrue(e1.isIdAssigned());
        assertTrue(e2.isIdAssigned());
        graph.commit();
        validateEdgeProperties(v1, e1, e2);

        pushChangesAndFlushCache();

        AtlasVertex<V,E> v1Loaded = graph.getVertex(v1.getId().toString());
        validateEdgeProperties(v1Loaded, e1, e2);
    }


    private <V, E> void validateEdgeProperties(AtlasVertex<V, E> v1, AtlasEdge<V, E> e1, AtlasEdge<V, E> e2) {
        assertEquals(e1.getId().toString(), v1.getProperty("edgeOne", String.class));
        assertEquals(e2.getId().toString(), v1.getProperty("edgeTwo", String.class));
        assertEquals(e1, v1.getProperty("edgeOne", AtlasEdge.class));
        assertEquals(e2, v1.getProperty("edgeTwo", AtlasEdge.class));
    }

    @Test
    public <V,E> void testLoadUnCachedEdgeWhenVertexCachedAndDeleted() {

        AtlasGraph<V, E> graph = getGraph();

        AtlasVertex<V, E> v1 = createVertex(graph);
        AtlasVertex<V, E> v2 = createVertex(graph);
        AtlasEdge<V,E> e1 = graph.addEdge(v1, v2, "e1");

        pushChangesAndFlushCache();

        AtlasVertex<V ,E> v1Loaded = graph.getVertex(v1.getId().toString());
        graph.removeVertex(v1Loaded);

        //previously this call would trigger a NullPointerException
        AtlasEdge<V,E> e1Loaded = graph.getEdge(e1.getId().toString());

    }

    @Test
    public <V, E> void testCannotRecommitMgmtTransaction() throws Exception {
        AtlasGraph<V, E> graph = getGraph();

        AtlasGraphManagement mgmt = graph.getManagementSystem();
        mgmt.makePropertyKey(RandomStringUtils.random(10), String.class, AtlasCardinality.SINGLE);
        try {
            mgmt.commit();
            fail("Expected failure did not occur.");
        }
        catch(Exception expected) {
            //good
        }
        try {
            mgmt.commit();
            fail("Recommitting the transaction is not allowed.");
        }
        catch(IllegalStateException expected) {
            //good
        }


    }
}
