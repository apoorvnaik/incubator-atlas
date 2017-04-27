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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.atlas.ibmgraph.IBMGraphEdge;
import org.apache.atlas.ibmgraph.IBMGraphVertex;
import org.apache.atlas.ibmgraph.api.json.update.ElementChanges;
import org.apache.atlas.ibmgraph.api.json.update.ElementIdListInfo;
import org.apache.atlas.ibmgraph.api.json.update.ElementType;
import org.apache.atlas.ibmgraph.api.json.update.UpdateScriptBinding;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.testng.annotations.Test;



/**
 *
 */
public class QueryBindingTest extends AbstractGraphDatabaseTest {

    //create new vertex and edge, set vertex/edge properties
    @Test
    public <V, E> void testCreateVerticesAndEdgesWithProperties() throws IOException {

        UpdateScriptBinding binding = new UpdateScriptBinding(getIbmGraphGraph().getTenantId());

        IBMGraphVertex v1 = new IBMGraphVertex(getIbmGraphGraph());
        IBMGraphVertex v2 = new IBMGraphVertex(getIbmGraphGraph());

        binding.addNewVertex(v1);
        binding.addNewVertex(v2);

        IBMGraphEdge e1 = new IBMGraphEdge(getIbmGraphGraph(), v2, v1, "knows");
        binding.addNewEdge(e1);

        ElementChanges changes = new ElementChanges(ElementType.vertex);
        changes.getPropertiesToSet().put("name", "Jeff");
        changes.getPropertiesToRemove().add("age");
        changes.setId(v1.getIdOrLocalId());

        ElementChanges changes2 = new ElementChanges(ElementType.vertex);
        changes2.getPropertiesToSet().put("name", "Corey");
        changes2.addNewMultiPropertyValue(Constants.TRAIT_NAMES_PROPERTY_KEY, "trait1");
        changes2.addNewMultiPropertyValue(Constants.TRAIT_NAMES_PROPERTY_KEY, "trait2");
        changes2.getPropertiesToRemove().add("age");
        changes2.setId(v2.getIdOrLocalId());

        ElementChanges edgeChanges = new ElementChanges(ElementType.edge);
        edgeChanges.setId(e1.getIdOrLocalId());
        edgeChanges.getPropertiesToSet().put("weight", "5");

        binding.getElementChangesList().add(changes);
        binding.getElementChangesList().add(changes2);

        binding.getElementChangesList().add(edgeChanges);
        getIbmGraphGraph().applyUpdates(binding);

        String v1Id = v1.getIdOrLocalId();
        String v2Id = v2.getIdOrLocalId();
        String e1Id = e1.getIdOrLocalId();

        //cached vertice and edges anre now stale.
        pushChangesAndFlushCache();

        AtlasVertex<?, ?> loadedV1 = getGraph().getVertex(v1Id);
        assertTrue(loadedV1.exists());
        assertEquals("Jeff", loadedV1.getProperty("name", String.class));
        assertTrue(getGraph().getVertex(v1Id).exists());

        AtlasVertex<?, ?> loadedV2 = getGraph().getVertex(v2Id);
        assertTrue(loadedV2.exists());
        assertEquals("Corey", loadedV2.getProperty("name", String.class));
        assertTrue(getGraph().getVertex(v2Id).exists());
        Collection<String> traitNames = loadedV2.getPropertyValues(Constants.TRAIT_NAMES_PROPERTY_KEY, String.class);
        assertEquals(2, traitNames.size());
        assertTrue(traitNames.contains("trait1"));
        assertTrue(traitNames.contains("trait2"));

        pushChangesAndFlushCache();
        AtlasEdge loadedEdge = getGraph().getEdge(e1Id);
        assertEquals(5, loadedEdge.getProperty("weight", Integer.class).intValue());
        assertTrue(loadedEdge.exists());

    }

    @Test
    public void testDeleteVertex() throws IOException {

        AtlasVertex v = getGraph().addVertex();
        AtlasVertex v2 = getGraph().addVertex();
        AtlasVertex v3 = getGraph().addVertex();
        AtlasVertex v4 = getGraph().addVertex();
        AtlasEdge e1 = getGraph().addEdge(v2, v3, "has");
        AtlasEdge e2 = getGraph().addEdge(v2, v4, "wants");
        getGraph().commit();

        UpdateScriptBinding binding = createUpdateScriptBinding();

        ElementChanges changes = new ElementChanges(ElementType.vertex);
        changes.setId(v.getId().toString());
        changes.setIsDelete(true);
        binding.getElementChangesList().add(changes);

        ElementChanges edgeChanges = new ElementChanges(ElementType.edge);
        edgeChanges.setId(e1.getId().toString());
        edgeChanges.setIsDelete(true);
        binding.getElementChangesList().add(edgeChanges);

        getIbmGraphGraph().applyUpdates(binding);

        this.pushChangesAndFlushCache();

        assertDeleted(v);
        assertDeleted(e1);

        assertExists(e2);
        assertExists(v2);
        assertExists(v3);
        assertExists(v4);
    }

    @Test
    public void testSetElementIdListProperty() throws IOException {
        AtlasVertex<IBMGraphVertex, IBMGraphEdge> v = getIbmGraphGraph().addVertex();
        AtlasVertex v2 = getGraph().addVertex();
        AtlasVertex v3 = getGraph().addVertex();
        AtlasVertex v4 = getGraph().addVertex();
        AtlasEdge e1 = getGraph().addEdge(v2, v3, "has");
        AtlasEdge e2 = getGraph().addEdge(v2, v4, "wants");
        getGraph().commit();


        UpdateScriptBinding binding = createUpdateScriptBinding();

        IBMGraphVertex newV1 = new IBMGraphVertex(getIbmGraphGraph());
        IBMGraphEdge newE1 = new IBMGraphEdge(getIbmGraphGraph(), v.getV(), newV1, "eats");

        binding.addNewVertex(newV1);
        binding.addNewEdge(newE1);

        ElementChanges changes = new ElementChanges(ElementType.vertex);
        changes.setId(v2.getId().toString());
        changes.getVertexIdListPropertiesToSet().add(new ElementIdListInfo("foo",
                Arrays.<String> asList(newV1.getIdOrLocalId(), v.getId().toString(), v4.getId().toString())));
        changes.getEdgeIdListPropertiesToSet()
                .add(new ElementIdListInfo("bar", Arrays.<String> asList(newE1.getIdOrLocalId())));

        binding.getElementChangesList().add(changes);

        ElementChanges edgeChanges = new ElementChanges(ElementType.edge);
        edgeChanges.setId(e1.getId().toString());
        edgeChanges.getVertexIdListPropertiesToSet().add(new ElementIdListInfo("foo",
                Arrays.<String> asList(newV1.getIdOrLocalId(), v.getId().toString(), v4.getId().toString())));
        edgeChanges.getEdgeIdListPropertiesToSet()
                .add(new ElementIdListInfo("bar", Arrays.<String> asList(newE1.getIdOrLocalId())));

        binding.getElementChangesList().add(edgeChanges);

        getIbmGraphGraph().applyUpdates(binding);

        pushChangesAndFlushCache();

        e1 = getGraph().getEdge(e1.getId().toString());
        v2 = getGraph().getVertex(v2.getId().toString());
        String edge1Id = newE1.getIdOrLocalId();
        AtlasEdge edge1 = getGraph().getEdge(edge1Id);
        String vertex1Id = newV1.getIdOrLocalId();
        AtlasVertex vertex1 = getGraph().getVertex(vertex1Id);

        List<AtlasVertex> vertices = v2.getListProperty("foo", AtlasVertex.class);
        assertEquals(3, vertices.size());

        assertEquals(vertex1Id, vertices.get(0).getId().toString());
        assertEquals(v.getId().toString(), vertices.get(1).getId().toString());
        assertEquals(v4.getId().toString(), vertices.get(2).getId().toString());

        List<AtlasEdge> edges = v2.getListProperty("bar", AtlasEdge.class);
        assertEquals(1, edges.size());
        assertEquals(edge1Id, edges.get(0).getId().toString());

        vertices = e1.getListProperty("foo", AtlasVertex.class);
        assertEquals(3, vertices.size());

        assertEquals(vertex1Id, vertices.get(0).getId().toString());
        assertEquals(v.getId().toString(), vertices.get(1).getId().toString());
        assertEquals(v4.getId().toString(), vertices.get(2).getId().toString());

        edges = e1.getListProperty("bar", AtlasEdge.class);
        assertEquals(1, edges.size());
        assertEquals(edge1Id, edges.get(0).getId().toString());

    }

    private UpdateScriptBinding createUpdateScriptBinding() {
        return new UpdateScriptBinding(getIbmGraphGraph().getTenantId());
    }

    @Test
    public void testSetElementIdProperty() throws IOException {

        AtlasVertex<IBMGraphVertex, IBMGraphEdge> v = getIbmGraphGraph().addVertex();
        AtlasVertex<IBMGraphVertex, IBMGraphEdge> v2 = getIbmGraphGraph().addVertex();
        AtlasEdge<IBMGraphVertex, IBMGraphEdge> e1 = getIbmGraphGraph().addEdge(v, v2, "has");
        getGraph().commit();

        UpdateScriptBinding binding = createUpdateScriptBinding();

        IBMGraphVertex newVertex = new IBMGraphVertex(getIbmGraphGraph());
        IBMGraphEdge newEdgeToCreate = new IBMGraphEdge(getIbmGraphGraph(), v.getV(), newVertex, "wants");
        binding.addNewVertex(newVertex);
        binding.addNewEdge(newEdgeToCreate);

        ElementChanges vertexChanges = new ElementChanges(ElementType.vertex);
        vertexChanges.setId(v.getId().toString());
        vertexChanges.getEdgeIdPropertiesToSet().put("e1Id", e1.getId().toString());
        vertexChanges.getEdgeIdPropertiesToSet().put("newEdgeId", newEdgeToCreate.getIdOrLocalId());
        vertexChanges.getVertexIdPropertiesToSet().put("v2Id", v2.getId().toString());
        vertexChanges.getVertexIdPropertiesToSet().put("newVertexId", newVertex.getIdOrLocalId());
        binding.getElementChangesList().add(vertexChanges);

        ElementChanges edgeChanges = new ElementChanges(ElementType.edge);
        edgeChanges.getEdgeIdPropertiesToSet().put("e1Id", e1.getId().toString());
        edgeChanges.getEdgeIdPropertiesToSet().put("newEdgeId", newEdgeToCreate.getIdOrLocalId());
        edgeChanges.getVertexIdPropertiesToSet().put("v2Id", v2.getId().toString());
        edgeChanges.getVertexIdPropertiesToSet().put("newVertexId", newVertex.getIdOrLocalId());
        edgeChanges.setId(e1.getId().toString());

        binding.getElementChangesList().add(edgeChanges);

        getIbmGraphGraph().applyUpdates(binding);

        pushChangesAndFlushCache();

        e1 = getIbmGraphGraph().getEdge(e1.getId().toString());
        v2 = getIbmGraphGraph().getVertex(v2.getId().toString());
        v = getIbmGraphGraph().getVertex(v.getId().toString());

        String newEdgeId = newEdgeToCreate.getIdOrLocalId();
        AtlasEdge newEdge = getIbmGraphGraph().getEdge(newEdgeId);
        String newVertexId = newVertex.getIdOrLocalId();
        newVertex = getIbmGraphGraph().getVertex(newVertexId).getV();

        assertEquals(newEdge.getId().toString(), e1.getProperty("newEdgeId", String.class));
        assertEquals(e1.getId().toString(), e1.getProperty("e1Id", String.class));
        assertEquals(newVertex.getId().toString(), e1.getProperty("newVertexId", String.class));
        assertEquals(v2.getId().toString(), e1.getProperty("v2Id", String.class));


        assertEquals(newEdge.getId().toString(), v.getProperty("newEdgeId", String.class));
        assertEquals(e1.getId().toString(), v.getProperty("e1Id", String.class));
        assertEquals(newVertex.getId().toString(), v.getProperty("newVertexId", String.class));
        assertEquals(v2.getId().toString(), v.getProperty("v2Id", String.class));
    }

    @Test
    public void testRemoveProperties() throws IOException {

        AtlasVertex v = getGraph().addVertex();
        v.setProperty("name", "Jeff");
        AtlasVertex v2 = getGraph().addVertex();
        AtlasEdge e1 = getGraph().addEdge(v, v2, "has");
        e1.setProperty("name", "e1");
        getGraph().commit();

        UpdateScriptBinding binding = createUpdateScriptBinding();

        ElementChanges changes1 = new ElementChanges(ElementType.vertex);
        changes1.setId(v.getId().toString());
        changes1.getPropertiesToRemove().add("name");
        binding.getElementChangesList().add(changes1);

        ElementChanges changes2 = new ElementChanges(ElementType.edge);
        changes2.setId(e1.getId().toString());
        changes2.getPropertiesToRemove().add("name");
        binding.getElementChangesList().add(changes2);

        getIbmGraphGraph().applyUpdates(binding);

        pushChangesAndFlushCache();

        v = getGraph().getVertex(v.getId().toString());
        assertFalse(v.getPropertyKeys().contains("name"));

        e1 = getGraph().getEdge(e1.getId().toString());
        assertFalse(v.getPropertyKeys().contains("name"));

    }
    private void assertDeleted(AtlasElement e) {
        if (e instanceof AtlasVertex<?, ?>) {
            AtlasVertex loaded = getGraph().getVertex(e.getId().toString());
            assertTrue(loaded == null || !loaded.exists());
        }
        if (e instanceof AtlasEdge<?, ?>) {
            AtlasEdge loaded = getGraph().getEdge(e.getId().toString());
            assertTrue(loaded == null || !loaded.exists());
        }

    }

    private void assertExists(AtlasElement e) {
        if (e instanceof AtlasVertex<?, ?>) {
            AtlasVertex loaded = getGraph().getVertex(e.getId().toString());
            assertTrue(loaded.exists());
        }
        if (e instanceof AtlasEdge<?, ?>) {
            AtlasEdge loaded = getGraph().getEdge(e.getId().toString());
            assertTrue(loaded.exists());
        }

    }

}
