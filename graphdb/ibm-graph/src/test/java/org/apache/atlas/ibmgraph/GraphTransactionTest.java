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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.Collection;
import java.util.List;

import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.commons.collections.IteratorUtils;
import org.testng.annotations.Test;

public class GraphTransactionTest extends AbstractGraphDatabaseTest {

    @Test
    public <V,E> void testPropertyValueChangesRevertedOnRollback() {

        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> v1 = createVertex(graph);
        AtlasVertex<V, E> v2 = createVertex(graph);
        AtlasEdge<V,E> e1 = graph.addEdge(v1,  v2,  "e1");
        v1.setProperty("name", "Joe");
        e1.setProperty("type", "big");
        graph.commit();
        assertEquals("Joe", v1.getProperty("name", String.class));
        assertEquals("big", String.valueOf(e1.getProperty("type", Object.class)));

        v1.setProperty("name", "James");
        e1.setProperty("type", "small");

        assertEquals("James", v1.getProperty("name", String.class));
        assertEquals("small", String.valueOf(e1.getProperty("type", Object.class)));

        graph.rollback();

        assertEquals("Joe", v1.getProperty("name", String.class));
        assertEquals("big", String.valueOf(e1.getProperty("type", Object.class)));

        pushChangesAndFlushCache();

        AtlasVertex<V, E> v1Copy = graph.getVertex(v1.getId().toString());
        AtlasEdge<V,E> e1Copy = graph.getEdge(e1.getId().toString());

        assertEquals("Joe", v1Copy.getProperty("name", String.class));
        assertEquals("big", e1Copy.getProperty("type", String.class));

    }

    @Test
    public <V,E> void testRemovedPropertiesRestoredOnRollback() {

        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> v1 = createVertex(graph);
        AtlasVertex<V, E> v2 = createVertex(graph);
        AtlasEdge<V,E> e1 = graph.addEdge(v1,  v2,  "e1");
        v1.setProperty("name", "Joe");
        e1.setProperty("weight", 15);
        graph.commit();
        v1.removeProperty("name");
        e1.removeProperty("weight");

        assertFalse(v1.getPropertyKeys().contains("name"));
        assertFalse(e1.getPropertyKeys().contains("weight"));

        graph.rollback();

        assertTrue(v1.getPropertyKeys().contains("name"));
        assertTrue(e1.getPropertyKeys().contains("weight"));

        pushChangesAndFlushCache();

        AtlasVertex<V, E> v1Copy = graph.getVertex(v1.getId().toString());
        AtlasEdge<V,E> e1Copy = graph.getEdge(e1.getId().toString());

        assertTrue(v1Copy.getPropertyKeys().contains("name"));
        assertTrue(e1Copy.getPropertyKeys().contains("weight"));

    }

    @Test
    public <V,E> void testNewPropertiesRemovedOnRollback() {
        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> v1 = createVertex(graph);
        AtlasVertex<V, E> v2 = createVertex(graph);
        AtlasEdge<V,E> e1 = graph.addEdge(v1,  v2,  "e1");
        graph.commit();
        v1.setProperty("name", "Joe");
        e1.setProperty("weight", 15);
        assertTrue(v1.getPropertyKeys().contains("name"));
        assertTrue(e1.getPropertyKeys().contains("weight"));

        graph.rollback();

        assertFalse(v1.getPropertyKeys().contains("name"));
        assertFalse(e1.getPropertyKeys().contains("weight"));

        pushChangesAndFlushCache();

        AtlasVertex<V, E> v1Copy = graph.getVertex(v1.getId().toString());
        AtlasEdge<V,E> e1Copy = graph.getEdge(e1.getId().toString());

        assertFalse(v1Copy.getPropertyKeys().contains("name"));
        assertFalse(e1Copy.getPropertyKeys().contains("weight"));

    }

    @Test
    public <V,E> void testNewMultiPropertyValueRemovedOnRollback() {
        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> v1 = createVertex(graph);
        v1.addProperty(Constants.TRAIT_NAMES_PROPERTY_KEY, "trait1");
        graph.commit();

        v1.addProperty(Constants.TRAIT_NAMES_PROPERTY_KEY, "trait2");
        Collection<String> values = v1.getPropertyValues(Constants.TRAIT_NAMES_PROPERTY_KEY, String.class);
        assertEquals(2, values.size());
        graph.rollback();

        values = v1.getPropertyValues(Constants.TRAIT_NAMES_PROPERTY_KEY, String.class);
        assertEquals(1, values.size());
        assertTrue(values.contains("trait1"));

        pushChangesAndFlushCache();

        AtlasVertex<V, E> v1Copy = graph.getVertex(v1.getId().toString());

        values = v1Copy.getPropertyValues(Constants.TRAIT_NAMES_PROPERTY_KEY, String.class);
        assertEquals(1, values.size());
        assertTrue(values.contains("trait1"));
    }

    @Test
    public <V,E> void testMultiPropertyOverwriteRolledBack() {
        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> v1 = createVertex(graph);
        v1.addProperty(Constants.TRAIT_NAMES_PROPERTY_KEY, "trait1");
        graph.commit();

        v1.removeProperty(Constants.TRAIT_NAMES_PROPERTY_KEY);
        v1.addProperty(Constants.TRAIT_NAMES_PROPERTY_KEY, "trait2");
        v1.addProperty(Constants.TRAIT_NAMES_PROPERTY_KEY, "trait3");
        Collection<String> values = v1.getPropertyValues(Constants.TRAIT_NAMES_PROPERTY_KEY, String.class);
        assertEquals(2, values.size());

        graph.rollback();

        values = v1.getPropertyValues(Constants.TRAIT_NAMES_PROPERTY_KEY, String.class);
        assertEquals(1, values.size());
        assertTrue(values.contains("trait1"));

        pushChangesAndFlushCache();

        AtlasVertex<V, E> v1Copy = graph.getVertex(v1.getId().toString());

        values = v1Copy.getPropertyValues(Constants.TRAIT_NAMES_PROPERTY_KEY, String.class);
        assertEquals(1, values.size());
        assertTrue(values.contains("trait1"));

    }

    @Test
    public <V,E> void testMultiPropertyOverwriteCommiited() {
        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> v1 = createVertex(graph);
        v1.addProperty(Constants.TRAIT_NAMES_PROPERTY_KEY, "trait1");
        graph.commit();
        v1.removeProperty(Constants.TRAIT_NAMES_PROPERTY_KEY);
        v1.addProperty(Constants.TRAIT_NAMES_PROPERTY_KEY, "trait2");
        v1.addProperty(Constants.TRAIT_NAMES_PROPERTY_KEY, "trait3");
        Collection<String> values = v1.getPropertyValues(Constants.TRAIT_NAMES_PROPERTY_KEY, String.class);
        assertEquals(2, values.size());

        graph.commit();

        values = v1.getPropertyValues(Constants.TRAIT_NAMES_PROPERTY_KEY, String.class);
        assertEquals(2, values.size());
        assertTrue(values.contains("trait2"));
        assertTrue(values.contains("trait3"));

        pushChangesAndFlushCache();

        AtlasVertex<V, E> v1Copy = graph.getVertex(v1.getId().toString());

        values = v1Copy.getPropertyValues(Constants.TRAIT_NAMES_PROPERTY_KEY, String.class);
        assertEquals(2, values.size());
        assertTrue(values.contains("trait2"));
        assertTrue(values.contains("trait3"));

    }

    @Test
    public <V,E> void testNormalPropertyClearRolledBack() {
        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> v1 = createVertex(graph);
        v1.setProperty("name", "Fred");
        graph.commit();

        v1.removeProperty("name");
        Collection<String> values = v1.getPropertyValues("name", String.class);
        assertEquals(0, values.size());

        graph.rollback();

        values = v1.getPropertyValues("name", String.class);
        assertEquals(1, values.size());
        assertTrue(values.contains("Fred"));

        pushChangesAndFlushCache();

        AtlasVertex<V, E> v1Copy = graph.getVertex(v1.getId().toString());

        values = v1Copy.getPropertyValues("name", String.class);
        assertEquals(1, values.size());
        assertTrue(values.contains("Fred"));

    }


    @Test
    public <V,E> void testRollbackVertexDeleteWhenNotPhysicallyCreated() {

        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> v1 = createVertex(graph);
        v1.setProperty("name", "Joe");

        graph.removeVertex(v1);

        assertFalse(v1.exists());

        graph.rollback();

        assertPropertyChangeFails(v1);

        assertFalse(v1.exists());
    }

    @Test
    public <V,E> void testRollbackVertexDeleteWhenPhysicallyCreated() {

        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> v1 = createVertex(graph);
        String id = v1.getId().toString();
        v1.setProperty("name", "Joe");
        graph.removeVertex(v1);
        assertFalse(v1.exists());

        graph.rollback();

        assertPropertyChangeFails(v1);
        assertFalse(v1.exists());

        pushChangesAndFlushCache();

        assertFalse(graph.getVertex(id).exists());

    }

    @Test
    public <V,E> void testRollbackEdgeDeleteWhenNotPhysicallyCreated() {

        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> v1 = createVertex(graph);
        AtlasVertex<V, E> v2 = createVertex(graph);
        AtlasEdge<V,E> e1 = graph.addEdge(v1,  v2,  "e1");

        v1.setProperty("name", "Joe");

        assertTrue(e1.exists());
        assertTrue(v1.exists());
        assertTrue(v2.exists());

        graph.removeEdge(e1);

        assertFalse(e1.exists());
        assertTrue(v1.exists());
        assertTrue(v2.exists());

        assertFalse(v1.isIdAssigned());
        assertFalse(v2.isIdAssigned());

        graph.rollback();

        assertPropertyChangeFails(e1);

        assertFalse(e1.exists());
        assertFalse(v1.exists());
        assertFalse(v2.exists());

    }

    @Test
    public <V,E> void testRollbackEdgeDeleteWhenPhysicallyCreated() {
        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> v1 = createVertex(graph);
        AtlasVertex<V, E> v2 = createVertex(graph);
        AtlasEdge<V,E> e1 = graph.addEdge(v1,  v2,  "e1");

        v1.setProperty("name", "Joe");
        String e1Id = e1.getId().toString();
        String v1Id = v1.getId().toString();
        String v2Id = v2.getId().toString();

        assertExists(e1);
        assertExists(v1);
        assertExists(v2);

        graph.removeEdge(e1);

        assertNotExists(e1);
        assertExists(v1);
        assertExists(v2);

        graph.rollback();

        assertPropertyChangeFails(e1);

        assertNotExists(e1);
        assertNotExists(v1);
        assertNotExists(v2);

        pushChangesAndFlushCache();

        assertNotExists(graph.getEdge(e1Id));
        assertNotExists(graph.getVertex(v1Id));
        assertNotExists(graph.getVertex(v2Id));
    }

    @Test
    public <V,E> void testRollbackVertexAddWhenNotPhysicallyCreated() {

        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> v1 = createVertex(graph);
        v1.setProperty("name", "Joe");

        assertTrue(v1.exists());
        assertFalse(v1.isIdAssigned());

        graph.rollback();
        assertFalse(v1.exists());
        assertPropertyChangeFails(v1);
    }

    @Test
    public <V,E> void testRollbackVertexAddWhenPhysicallyCreated() {

        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> v1 = createVertex(graph);
        String id = v1.getId().toString();
        v1.setProperty("name", "Joe");

        assertExists(v1);
        assertTrue(v1.isIdAssigned());

        graph.rollback();

        assertNotExists(v1);
        assertPropertyChangeFails(v1);

        pushChangesAndFlushCache();

        assertNotExists(graph.getVertex(id));

    }

    @Test
    public <V,E> void testRollbackEdgeAddWhenPhysicallyCreated() {
        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> v1 = createVertex(graph);
        AtlasVertex<V, E> v2 = createVertex(graph);
        AtlasEdge<V,E> e1 = graph.addEdge(v1,  v2,  "e1");

        String e1Id = e1.getId().toString();
        String v1Id = v1.getId().toString();
        String v2Id = v2.getId().toString();

        v1.setProperty("name", "Joe");

        assertExists(e1);
        assertExists(v1);
        assertExists(v2);

        assertTrue(e1.isIdAssigned());
        assertTrue(v1.isIdAssigned());
        assertTrue(v2.isIdAssigned());

        graph.rollback();

        assertPropertyChangeFails(e1);

        assertNotExists(e1);
        assertNotExists(v1);
        assertNotExists(v2);
        pushChangesAndFlushCache();

        assertNotExists(graph.getEdge(e1Id));
        assertNotExists(graph.getVertex(v1Id));
        assertNotExists(graph.getVertex(v2Id));
    }

    @Test
    public <V,E> void testRollbackEdgeAddWhenNotPhysicallyCreated() {
        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> v1 = createVertex(graph);
        AtlasVertex<V, E> v2 = createVertex(graph);
        AtlasEdge<V,E> e1 = graph.addEdge(v1,  v2,  "e1");

        v1.setProperty("name", "Joe");

        assertExists(e1);
        assertExists(v1);
        assertExists(v2);

        assertFalse(e1.isIdAssigned());
        assertFalse(v1.isIdAssigned());
        assertFalse(v2.isIdAssigned());

        graph.rollback();

        assertPropertyChangeFails(e1);
        assertPropertyChangeFails(v1);
        assertPropertyChangeFails(v2);

        assertNotExists(e1);
        assertNotExists(v1);
        assertNotExists(v2);

        String v1Id;
        String v2Id;
        String e1Id;

        //v1 was not physically created, so it has no id
        assertNull(v1.getId());
        //v2 was not physically created, so it has no id
        assertNull(v2.getId());
        //e1 was not physically created, so it has no id
        assertNull(e1.getId());
    }

    @Test
    public <V,E> void testRollbackDeleteOfExistingEdge() {

        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> v1 = createVertex(graph);
        AtlasVertex<V, E> v2 = createVertex(graph);
        AtlasEdge<V,E> e1 = graph.addEdge(v1,  v2,  "e1");

        v1.setProperty("name", "Joe");

        assertExists(e1);
        assertExists(v1);
        assertExists(v2);

        graph.commit();

        assertExists(e1);
        assertExists(v1);
        assertExists(v2);

        graph.removeEdge(e1);

        assertNotExists(e1);
        assertExists(v1);
        assertExists(v2);

        graph.rollback();

        assertExists(e1);
        assertExists(v1);
        assertExists(v2);
    }


    public <V,E> void testRollbackDeleteOfExistingVertex() {
        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> v1 = createVertex(graph);
        String id = v1.getId().toString();
        v1.setProperty("name", "Joe");
        graph.commit();
        graph.removeVertex(v1);
        assertFalse(v1.exists());
        graph.rollback();
        try {
            v1.setProperty("name", "George");
            fail("You should not be able to modify a deleted vertex.");
        }
        catch(IllegalStateException expected) {

        }

        assertFalse(v1.exists());

        pushChangesAndFlushCache();

        assertFalse(graph.getVertex(id).exists());

    }

    //these are enhanced versions of existing (but unmerged) tests in GraphTransactionTest.  The
    //changes are purely additive.


    private <T> void assertContains(Iterable<T> collection, T toFind) {
        List<T> list = IteratorUtils.toList(collection.iterator());
        assertTrue(list.contains(toFind));
    }

    private <T> void assertDoesNotContain(Iterable<T> collection, T toFind) {
        List<T> list = IteratorUtils.toList(collection.iterator());
        assertFalse(list.contains(toFind));
    }

    @Test
    public <V,E> void testEdgeAddIsolation() throws Throwable {
        AtlasGraph<V, E> graph = getGraph();
        final AtlasVertex<V, E> v1 = createVertex(graph);
        final AtlasVertex<V, E> v2 = createVertex(graph);

        graph.commit();

        final AtlasEdge<V,E> e1 = graph.addEdge(v1,  v2,  "e1");

        runSynchronouslyInNewThread(new Runnable() {

            @Override
            public void run() {
                //these actions run in a different transaction, since it's a different thread

                graph.removeVertex(v1);
                graph.removeVertex(v2);
                assertTrue(e1.exists()); //e1 should not be modified, it is not known to this transaction
                assertFalse(v1.exists());
                assertFalse(v2.exists());
            }
        });
    }

    @Test
    public <V,E> void testEdgeRemovalIsolation() throws Throwable {

        AtlasGraph<V, E> graph = getGraph();
        final AtlasVertex<V, E> v1 = createVertex(graph);
        final AtlasVertex<V, E> v2 = createVertex(graph);
        final AtlasEdge<V,E> e1 = graph.addEdge(v1,  v2,  "e1");
        graph.commit();

        graph.removeEdge(e1);

        //TODO: test delete vertex/edge isolation, once delete task is merged
        runSynchronouslyInNewThread(new Runnable() {

            @Override
            public void run() {
                //these actions run in a different transaction, since it's a different thread

                assertTrue(e1.exists());
                graph.removeVertex(v1);
                assertFalse(e1.exists()); //e1 should be implicitly deleted when v1 is deleted
                assertFalse(v1.exists());
                graph.rollback();

                assertTrue(e1.exists());
                assertTrue(v1.exists());
                assertTrue(v2.exists());

                graph.removeVertex(v2);
                assertFalse(v2.exists());
            }
        });
    }

    @Test
    public <V,E> void testTransactionIsolation() throws Throwable {

        AtlasGraph<V, E> graph = getGraph();
        final AtlasVertex<V, E> v1 = createVertex(graph);
        final AtlasVertex<V, E> v2 = createVertex(graph);
        final AtlasVertex<V, E> v3 = createVertex(graph);
        v1.setProperty("name", "Joe");
        v1.setProperty("favoriteColor", "blue");

        final AtlasEdge<V,E> e1 = graph.addEdge(v1,  v2, "e1");
        graph.commit();

        graph.removeEdge(e1);
        graph.removeVertex(v3);

        final AtlasVertex<V, E> v4 = createVertex(graph);

        v1.setProperty("size", "large");
        v1.setProperty("favoriteColor", "red");

        final AtlasEdge<V,E> e2 = graph.addEdge(v1,  v2,  "e2");

        assertContains(v2.getEdges(AtlasEdgeDirection.IN), e2);
        assertContains(v1.getEdges(AtlasEdgeDirection.OUT), e2);

        assertEquals("Joe", v1.getProperty("name", String.class));
        assertEquals("red", v1.getProperty("favoriteColor", String.class));
        assertTrue(v4.exists());
        assertFalse(e1.exists());
        assertFalse(v3.exists());

        runSynchronouslyInNewThread(new Runnable() {

            @Override
            public void run() {
                //this thread should not see the changes that were made in
                //the main thread.  The thread should have its own transaction.

                assertTrue(e1.exists());
                assertTrue(v3.exists());
                assertDoesNotContain(v2.getEdges(AtlasEdgeDirection.IN), e2);
                assertDoesNotContain(v1.getEdges(AtlasEdgeDirection.OUT), e2);
                assertFalse(v1.getPropertyKeys().contains("size"));
                assertEquals("blue", v1.getProperty("favoriteColor", String.class));

            }
        });
    }

    @Test
    public <V,E> void testCommittedChangesSeenByOtherTransactions() throws Throwable {

        AtlasGraph<V, E> graph = getGraph();
        final AtlasVertex<V, E> v1 = createVertex(graph);
        final AtlasVertex<V, E> v2 = createVertex(graph);
        final AtlasVertex<V, E> v3 = createVertex(graph);
        v1.setProperty("name", "Joe");
        v1.setProperty("favoriteColor", "blue");
        final AtlasEdge<V,E> e1 = graph.addEdge(v1,  v2, "e1");
        graph.commit();
        final String v1Id = v1.getId().toString();
        final String v3Id = v3.getId().toString();
        final String e1Id = e1.getId().toString();

        runSynchronouslyInNewThread(new Runnable() {

            @Override
            public void run() {
                //this thread should not see the changes that were made in
                //the main thread.  The thread should have its own transaction.
                AtlasVertex v1 = graph.getVertex(v1Id);
                AtlasVertex v3 = graph.getVertex(v3Id);
                AtlasEdge e1 = graph.getEdge(e1Id);
                v1.setProperty("name", "Bob");
                v1.removeProperty("favoriteColor");
                graph.removeEdge(e1);
                graph.removeVertex(v3);
                graph.commit();
            }
        });

        //check that the committed updates are seen by the original transaction

        assertEquals("Bob", v1.getProperty("name", String.class));
        assertFalse(v1.getPropertyKeys().contains("favoritesColor"));
        assertTrue(v1.exists());
        assertFalse(e1.exists());
        assertFalse(v3.exists());

    }


    private <V, E> void assertPropertyChangeFails(AtlasElement element) {
        try {
            element.setProperty("name", "George");
            fail("You should not be able to modify a deleted element.");
        }
        catch(IllegalStateException expected) {

        }
    }

    private void assertNotExists(AtlasElement element) {
        assertTrue(element == null || ! element.exists());
    }

    private void assertExists(AtlasElement element) {
        assertTrue(element.exists());
    }
}
