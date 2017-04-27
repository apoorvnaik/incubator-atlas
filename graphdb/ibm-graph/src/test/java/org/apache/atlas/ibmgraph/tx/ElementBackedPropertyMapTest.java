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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.atlas.ibmgraph.IBMGraphGraph;
import org.apache.atlas.ibmgraph.IBMGraphVertex;
import org.apache.atlas.ibmgraph.api.action.IPropertyValue;
import org.apache.atlas.ibmgraph.api.json.JsonVertex;
import org.apache.atlas.ibmgraph.api.json.PropertyValue;
import org.apache.atlas.ibmgraph.tx.ElementBackedPropertyMap;
import org.apache.atlas.ibmgraph.tx.IBMGraphTransaction;
import org.testng.annotations.Test;

public class ElementBackedPropertyMapTest {

    @Test
    public void testGetUnmodifiedProperty() {
        IBMGraphGraph graph = createMockGraph();
        JsonVertex jsonVertex  = new JsonVertex();
        jsonVertex.setId("id");
        jsonVertex.setLabel("label");
        Set<PropertyValue> fred = addProperty(jsonVertex, "name", "Fred");
        IBMGraphVertex vertex = new IBMGraphVertex(graph, jsonVertex);
        ElementBackedPropertyMap map = new ElementBackedPropertyMap(vertex);

        //first get unmodified value
        Set<IPropertyValue> found = map.get("name");
        assertTrue(map.containsKey("name"));
        assertFalse(map.containsKey("year"));
        assertEquals(found, fred);

        Set george = createSinglePropertyValue("George");
        map.put("name",george);
        assertEquals(george, map.get("name"));
        assertTrue(map.containsKey("name"));

        map.clearChanges();
        assertEquals(fred, map.get("name"));

        //should go back to pointing to Fred

        //make sure original vertex was unmodified
        ElementBackedPropertyMap map2 = new ElementBackedPropertyMap(vertex);
        Set<IPropertyValue> found2 = map2.get("name");
        assertEquals(found2, fred);
    }

    @Test
    public void testClearMap() {

        IBMGraphGraph graph = createMockGraph();
        JsonVertex jsonVertex  = new JsonVertex();
        jsonVertex.setId("id");
        jsonVertex.setLabel("label");
        Set<PropertyValue> fred = addProperty(jsonVertex, "name", "Fred");
        IBMGraphVertex vertex = new IBMGraphVertex(graph, jsonVertex);
        ElementBackedPropertyMap map = new ElementBackedPropertyMap(vertex);

        //first get unmodified value
        Set<IPropertyValue> found = map.get("name");
        assertEquals(found, fred);

        map.clear();

        assertNull(map.get("name"));
        assertEquals(0, map.size());
        assertFalse(map.containsKey("name"));
        //ensure that original vertex was not changed

        ElementBackedPropertyMap map2 = new ElementBackedPropertyMap(vertex);
        Set<IPropertyValue> found2 = map2.get("name");
        assertEquals(found2, fred);

        map.clearChanges();

        //map should now reflect the properties in the underlying vertex
        assertEquals(1, map.size());
        assertEquals(fred, map.get("name"));

    }

    @Test
    public void testRemoveElementFromMap() {
        IBMGraphGraph graph = createMockGraph();
        JsonVertex jsonVertex  = new JsonVertex();
        jsonVertex.setId("id");
        jsonVertex.setLabel("label");
        Set<PropertyValue> fred = addProperty(jsonVertex, "name", "Fred");
        Set<PropertyValue> ibm = addProperty(jsonVertex, "company", "IBM");
        IBMGraphVertex vertex = new IBMGraphVertex(graph, jsonVertex);
        ElementBackedPropertyMap map = new ElementBackedPropertyMap(vertex);

        //first get unmodified value
        Set<IPropertyValue> found = map.get("name");
        assertEquals(found, fred);

        map.remove("name");

        assertNull(map.get("name"));
        assertEquals(1, map.size());
        assertFalse(map.containsKey("name"));

        assertEquals(ibm, map.get("company"));
        assertTrue(map.containsKey("company"));

        //ensure that original vertex was not changed

        ElementBackedPropertyMap map2 = new ElementBackedPropertyMap(vertex);
        Set<IPropertyValue> found2 = map2.get("name");
        assertEquals(found2, fred);

    }

    @Test
    public void testAddNewMapEntry() {
        IBMGraphGraph graph = createMockGraph();
        JsonVertex jsonVertex  = new JsonVertex();
        jsonVertex.setId("id");
        jsonVertex.setLabel("label");
        addProperty(jsonVertex, "name", "Fred");
        addProperty(jsonVertex, "company", "IBM");
        IBMGraphVertex vertex = new IBMGraphVertex(graph, jsonVertex);
        ElementBackedPropertyMap map = new ElementBackedPropertyMap(vertex);

        Set yellow = createSinglePropertyValue("yellow");
        map.put("favoriteColor", yellow);
        assertTrue(map.containsKey("favoriteColor"));
        assertEquals(yellow, map.get("favoriteColor"));

    }


    @Test
    public void testReturnedKeySetReflectsNewChanges() {

        IBMGraphGraph graph = createMockGraph();
        JsonVertex jsonVertex  = new JsonVertex();
        jsonVertex.setId("id");
        jsonVertex.setLabel("label");
        addProperty(jsonVertex, "name", "Fred");
        addProperty(jsonVertex, "company", "IBM");
        addProperty(jsonVertex, "nickname", "Fred");
        IBMGraphVertex vertex = new IBMGraphVertex(graph, jsonVertex);
        ElementBackedPropertyMap map = new ElementBackedPropertyMap(vertex);

        Set<String> keys = map.keySet();
        assertFalse(keys.contains("favoriteColor"));
        assertTrue(keys.contains("name"));
        assertTrue(keys.contains("nickname"));
        //make some changes

        Set yellow = createSinglePropertyValue("yellow");
        map.put("favoriteColor", yellow);

        Set freddie = createSinglePropertyValue("Freddie");
        map.remove("name");
        map.put("favoriteColor", yellow);
        map.put("nickname", freddie);

        //check that the keyset is still correct
        assertTrue(keys.contains("favoriteColor"));
        assertFalse(keys.contains("name"));
        assertTrue(keys.contains("nickname"));

    }

    @Test
    public void testReturnedEntrySetReflectsNewChanges() {

        IBMGraphGraph graph = createMockGraph();
        JsonVertex jsonVertex  = new JsonVertex();
        jsonVertex.setId("id");
        jsonVertex.setLabel("label");
        Set fred = addProperty(jsonVertex, "name", "Fred");
        Set ibm = addProperty(jsonVertex, "company", "IBM");
        Set fredNickname = addProperty(jsonVertex, "nickname", "Fred");
        IBMGraphVertex vertex = new IBMGraphVertex(graph, jsonVertex);
        ElementBackedPropertyMap map = new ElementBackedPropertyMap(vertex);

        Set<Map.Entry<String,Set<IPropertyValue>>> entries = map.entrySet();
        for(Map.Entry<String, Set<IPropertyValue>> entry : entries) {

            if(entry.getKey().equals("name")) {
                assertEquals(fred, entry.getValue());
            }
            else if(entry.getKey().equals("nickname")) {
                assertEquals(fredNickname, entry.getValue());
            }
            else if(entry.getKey().equals("company")) {
                assertEquals(ibm, entry.getValue());
            }
            else {
                fail("Unexpected entry: " + entry);
            }
        }

        //make some changes
        Set freddie = createSinglePropertyValue("Freddie");
        map.put("nickname", freddie);
        Set yellow = createSinglePropertyValue("yellow");
        map.put("favoriteColor", yellow);
        map.remove("name");

        //check that the keyset is still correct
        for(Map.Entry<String, Set<IPropertyValue>> entry : entries) {

            if(entry.getKey().equals("favoriteColor")) {
                assertEquals(yellow, entry.getValue());
            }
            else if(entry.getKey().equals("company")) {
                assertEquals(ibm, entry.getValue());
            }
            else if(entry.getKey().equals("nickname")) {
                assertEquals(freddie, entry.getValue());
            }
            else {
                fail("Unexpected entry: " + entry);
            }
        }
    }


    @Test
    public void testApplyChanges() {
        IBMGraphGraph graph = createMockGraph();
        JsonVertex jsonVertex  = new JsonVertex();
        jsonVertex.setId("id");
        jsonVertex.setLabel("label");
        Set fred = addProperty(jsonVertex, "name", "Fred");
        Set ibm = addProperty(jsonVertex, "company", "IBM");
        IBMGraphVertex vertex = new IBMGraphVertex(graph, jsonVertex);
        ElementBackedPropertyMap map = new ElementBackedPropertyMap(vertex);

        Set yellow = createSinglePropertyValue("yellow");
        map.put("favoriteColor", yellow);
        map.remove("name");
        assertFalse(map.containsKey("name"));
        assertEquals(yellow, map.get("favoriteColor"));
        map.applyChanges((Map)jsonVertex.getProperties());

        IBMGraphVertex vertex2 = new IBMGraphVertex(graph, jsonVertex);
        ElementBackedPropertyMap map2 = new ElementBackedPropertyMap(vertex2);

        assertEquals(map, map2);

        map.clearChanges();

        assertFalse(map.containsKey("favoriteColor"));
        assertEquals(fred, map.get("name"));
        assertEquals(ibm, map.get("company"));
    }

    private IBMGraphGraph createMockGraph() {
        IBMGraphGraph graph = mock(IBMGraphGraph.class);
        IBMGraphTransaction tx = new IBMGraphTransaction(graph);
        when(graph.getTx()).thenReturn(tx);
        return graph;
    }

    private Set<PropertyValue> addProperty(JsonVertex vertex, String name, Object value) {
        Set<PropertyValue> set = createSinglePropertyValue(value);
        vertex.getProperties().put(name, set);
        return set;
    }

    private Set<PropertyValue> createSinglePropertyValue(Object value) {
        PropertyValue valueToSet = new PropertyValue(null, value);
        Set<PropertyValue> set = Collections.singleton(valueToSet);
        return set;
    }
}
