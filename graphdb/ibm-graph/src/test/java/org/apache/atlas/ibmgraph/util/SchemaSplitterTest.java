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
package org.apache.atlas.ibmgraph.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.atlas.ibmgraph.IBMGraphPropertyKey;
import org.apache.atlas.ibmgraph.api.Cardinality;
import org.apache.atlas.ibmgraph.api.json.Index;
import org.apache.atlas.ibmgraph.api.json.PropertyDataType;
import org.apache.atlas.ibmgraph.api.json.PropertyKey;
import org.apache.atlas.ibmgraph.api.json.Schema;
import org.apache.atlas.ibmgraph.api.json.Index.IndexType;
import org.apache.atlas.ibmgraph.api.json.Schema.IndexDefinition;
import org.apache.atlas.ibmgraph.util.GraphDBUtil;
import org.apache.atlas.ibmgraph.util.SchemaSplitter;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.testng.annotations.Test;


/**
 *
 */
public class SchemaSplitterTest {

    //Test cases:
    // mixture of new and existing property keys -- composite
    // mixture of new and existing property keys -- mixed

    @Test
    public void testCreateOneSimpleSchema() {

        int maxBatchSize = 10;
        Map<String, AtlasPropertyKey> pendingPropertyKeys = makeTestMap("name","age");
        Map<String,Index> pendingIndices = new HashMap<>();
        addCompositeEdgeIndex(pendingIndices, "composite-edge-index", pendingPropertyKeys.get("name"));
        addCompositeVertexIndex(pendingIndices, "composite-vertex-index", pendingPropertyKeys.get("name"));
        addMixedEdgeIndex(pendingIndices, "mixed-edge-index", pendingPropertyKeys.get("name"), pendingPropertyKeys.get("age"));
        addMixedVertexIndex(pendingIndices, "mixed-vertex-index", pendingPropertyKeys.get("name"), pendingPropertyKeys.get("age"));

        SchemaSplitter splitter = new SchemaSplitter(maxBatchSize, pendingPropertyKeys, pendingIndices, Collections.emptySet());
        List<Schema> batches = splitter.createSchemaUpdateBatches();
        assertEquals(1, batches.size());
        Schema firstSchema = batches.get(0);
        assertEquals(2, firstSchema.getEdgeIndexes().size());
        assertEquals(2, firstSchema.getVertexIndexes().size());
        assertEquals(2, firstSchema.getPropertyKeys().size());

    }


    @Test
    public void testCompositeIndexDeferredToSecondBatch() {

        int maxBatchSize = 1;
        Map<String, AtlasPropertyKey> pendingPropertyKeys = makeTestMap("name","age");
        Map<String,Index> pendingIndices = new HashMap<>();
        addCompositeVertexIndex(pendingIndices, "composite-vertex-index", pendingPropertyKeys.get("name"), pendingPropertyKeys.get("age"));
        addCompositeEdgeIndex(pendingIndices, "composite-edge-index", pendingPropertyKeys.get("name"), pendingPropertyKeys.get("age"));

        SchemaSplitter splitter = new SchemaSplitter(maxBatchSize, pendingPropertyKeys, pendingIndices, Collections.emptySet());
        List<Schema> batches = splitter.createSchemaUpdateBatches();
        assertEquals(2, batches.size());
        Schema firstSchema = batches.get(0);
        //the indices cannot be created in the first batch since only one property key has been
        //added at that point.
        assertEquals(0, firstSchema.getVertexIndexes().size());
        assertEquals(1, firstSchema.getPropertyKeys().size());
        assertEquals("age", firstSchema.getPropertyKeys().iterator().next().getName());

        Schema secondSchema = batches.get(1);
        assertEquals(1, secondSchema.getPropertyKeys().size());
        assertEquals(1, secondSchema.getVertexIndexes().size());
        assertEquals(1, secondSchema.getEdgeIndexes().size());
        IndexDefinition vertexIndexDef = secondSchema.getVertexIndexes().get(0);
        assertEquals("composite-vertex-index", vertexIndexDef.getName());
        assertEquals(2, vertexIndexDef.getPropertyKeys().size());
        assertTrue(vertexIndexDef.getPropertyKeys().contains("name"));
        assertTrue(vertexIndexDef.getPropertyKeys().contains("age"));


        IndexDefinition edgeIndexDef = secondSchema.getEdgeIndexes().get(0);
        assertEquals("composite-edge-index", edgeIndexDef.getName());
        assertEquals(2, edgeIndexDef.getPropertyKeys().size());
        assertTrue(edgeIndexDef.getPropertyKeys().contains("name"));
        assertTrue(edgeIndexDef.getPropertyKeys().contains("age"));

    }


    @Test
    public void testCreateCompositeIndexWithOnlyExistingPropertyKeys() {

        int maxBatchSize = 1;
        Map<String, AtlasPropertyKey> existingPropertyKeys = makeTestMap("name","age");
        Map<String,Index> pendingIndices = new HashMap<>();
        addCompositeVertexIndex(pendingIndices, "composite-vertex-index", existingPropertyKeys.get("name"), existingPropertyKeys.get("age"));
        addCompositeEdgeIndex(pendingIndices, "composite-edge-index", existingPropertyKeys.get("name"), existingPropertyKeys.get("age"));

        SchemaSplitter splitter = new SchemaSplitter(maxBatchSize, Collections.emptyMap(), pendingIndices, new HashSet<>(Arrays.asList("name","age")));
        List<Schema> batches = splitter.createSchemaUpdateBatches();
        assertEquals(1, batches.size());
        Schema firstSchema = batches.get(0);

        assertEquals(1, firstSchema.getVertexIndexes().size());
        assertEquals(1, firstSchema.getEdgeIndexes().size());
        IndexDefinition vertexIndexDef = firstSchema.getVertexIndexes().get(0);
        assertEquals("composite-vertex-index", vertexIndexDef.getName());
        assertEquals(2, vertexIndexDef.getPropertyKeys().size());
        assertTrue(vertexIndexDef.getPropertyKeys().contains("name"));
        assertTrue(vertexIndexDef.getPropertyKeys().contains("age"));

        IndexDefinition edgeIndexDef = firstSchema.getEdgeIndexes().get(0);
        assertEquals("composite-edge-index", edgeIndexDef.getName());
        assertEquals(2, edgeIndexDef.getPropertyKeys().size());
        assertTrue(edgeIndexDef.getPropertyKeys().contains("name"));
        assertTrue(edgeIndexDef.getPropertyKeys().contains("age"));

    }


    @Test
    public void testAddOnlyExistingPropertyKeysToMixedIndex() {

        int maxBatchSize = 1;
        Map<String, AtlasPropertyKey> existingPropertyKeys = makeTestMap("name","age");
        Map<String,Index> pendingIndices = new HashMap<>();
        addMixedVertexIndex(pendingIndices, "mixed-vertex-index", existingPropertyKeys.get("name"), existingPropertyKeys.get("age"));
        addMixedEdgeIndex(pendingIndices, "mixed-edge-index", existingPropertyKeys.get("name"), existingPropertyKeys.get("age"));

        SchemaSplitter splitter = new SchemaSplitter(maxBatchSize, Collections.emptyMap(), pendingIndices, new HashSet<>(Arrays.asList("name","age")));
        List<Schema> batches = splitter.createSchemaUpdateBatches();
        assertEquals(1, batches.size());
        Schema firstSchema = batches.get(0);

        assertEquals(1, firstSchema.getVertexIndexes().size());
        assertEquals(1, firstSchema.getEdgeIndexes().size());
        IndexDefinition vertexIndexDef = firstSchema.getVertexIndexes().get(0);
        assertEquals("mixed-vertex-index", vertexIndexDef.getName());
        assertEquals(2, vertexIndexDef.getPropertyKeys().size());
        assertTrue(vertexIndexDef.getPropertyKeys().contains("name"));
        assertTrue(vertexIndexDef.getPropertyKeys().contains("age"));

        IndexDefinition edgeIndexDef = firstSchema.getEdgeIndexes().get(0);
        assertEquals("mixed-edge-index", edgeIndexDef.getName());
        assertEquals(2, edgeIndexDef.getPropertyKeys().size());
        assertTrue(edgeIndexDef.getPropertyKeys().contains("name"));
        assertTrue(edgeIndexDef.getPropertyKeys().contains("age"));
    }

    @Test
    public void testPropertyKeyAssociatedWithMultipleIndices() {

        int maxBatchSize = 1;
        Map<String, AtlasPropertyKey> newPropertyKeys = makeTestMap("name","age");
        Map<String,Index> pendingIndices = new HashMap<>();
        addMixedVertexIndex(pendingIndices, "mixed-vertex-index1", newPropertyKeys.get("name"), newPropertyKeys.get("age"));
        addMixedVertexIndex(pendingIndices, "mixed-vertex-index2", newPropertyKeys.get("name"), newPropertyKeys.get("age"));
        addMixedEdgeIndex(pendingIndices, "mixed-edge-index1", newPropertyKeys.get("name"), newPropertyKeys.get("age"));
        addMixedEdgeIndex(pendingIndices, "mixed-edge-index2", newPropertyKeys.get("name"), newPropertyKeys.get("age"));
        addCompositeVertexIndex(pendingIndices, "composite-vertex-index1", newPropertyKeys.get("name"), newPropertyKeys.get("age"));
        addCompositeVertexIndex(pendingIndices, "composite-vertex-index2", newPropertyKeys.get("name"), newPropertyKeys.get("age"));
        addCompositeEdgeIndex(pendingIndices, "composite-edge-index1", newPropertyKeys.get("name"), newPropertyKeys.get("age"));
        addCompositeEdgeIndex(pendingIndices, "composite-edge-index2", newPropertyKeys.get("name"), newPropertyKeys.get("age"));
        SchemaSplitter splitter = new SchemaSplitter(maxBatchSize, newPropertyKeys, pendingIndices, Collections.emptySet());
        List<Schema> batches = splitter.createSchemaUpdateBatches();
        assertEquals(2, batches.size());
        {
            Schema firstSchema = batches.get(0);

            assertEquals(2, firstSchema.getVertexIndexes().size());
            assertEquals(2, firstSchema.getEdgeIndexes().size());

            assertEquals(1, firstSchema.getPropertyKeys().size());
            assertEquals("age", firstSchema.getPropertyKeys().iterator().next().getName());

            IndexDefinition vertexIndexDef1 = firstSchema.getVertexIndexes().get(0);
            assertEquals("mixed-vertex-index1", vertexIndexDef1.getName());
            assertEquals(1, vertexIndexDef1.getPropertyKeys().size());
            assertTrue(vertexIndexDef1.getPropertyKeys().contains("age"));

            IndexDefinition vertexIndexDef2 = firstSchema.getVertexIndexes().get(1);
            assertEquals("mixed-vertex-index2", vertexIndexDef2.getName());
            assertEquals(1, vertexIndexDef2.getPropertyKeys().size());
            assertTrue(vertexIndexDef2.getPropertyKeys().contains("age"));

            IndexDefinition edgeIndexDef1 = firstSchema.getEdgeIndexes().get(0);
            assertEquals("mixed-edge-index1", edgeIndexDef1.getName());
            assertEquals(1, edgeIndexDef1.getPropertyKeys().size());
            assertTrue(edgeIndexDef1.getPropertyKeys().contains("age"));

            IndexDefinition edgeIndexDef2 = firstSchema.getEdgeIndexes().get(1);
            assertEquals("mixed-edge-index2", edgeIndexDef2.getName());
            assertEquals(1, edgeIndexDef2.getPropertyKeys().size());
            assertTrue(edgeIndexDef2.getPropertyKeys().contains("age"));
        }

        {
            Schema secondSchema = batches.get(1);

            assertEquals(4, secondSchema.getVertexIndexes().size());
            assertEquals(4, secondSchema.getEdgeIndexes().size());

            assertEquals(1, secondSchema.getPropertyKeys().size());
            assertEquals("name", secondSchema.getPropertyKeys().iterator().next().getName());

            IndexDefinition vertexIndexDef1 = secondSchema.getVertexIndexes().get(0);
            assertEquals("mixed-vertex-index1", vertexIndexDef1.getName());
            assertEquals(1, vertexIndexDef1.getPropertyKeys().size());
            assertTrue(vertexIndexDef1.getPropertyKeys().contains("name"));

            IndexDefinition vertexIndexDef2 = secondSchema.getVertexIndexes().get(1);
            assertEquals("composite-vertex-index2", vertexIndexDef2.getName());
            assertEquals(2, vertexIndexDef2.getPropertyKeys().size());
            assertTrue(vertexIndexDef2.getPropertyKeys().contains("name"));
            assertTrue(vertexIndexDef2.getPropertyKeys().contains("age"));

            IndexDefinition vertexIndexDef3 = secondSchema.getVertexIndexes().get(2);
            assertEquals("mixed-vertex-index2", vertexIndexDef3.getName());
            assertEquals(1, vertexIndexDef3.getPropertyKeys().size());
            assertTrue(vertexIndexDef3.getPropertyKeys().contains("name"));

            IndexDefinition vertexIndexDef4 = secondSchema.getVertexIndexes().get(3);
            assertEquals("composite-vertex-index1", vertexIndexDef4.getName());
            assertEquals(2, vertexIndexDef4.getPropertyKeys().size());
            assertTrue(vertexIndexDef4.getPropertyKeys().contains("name"));
            assertTrue(vertexIndexDef4.getPropertyKeys().contains("age"));


            IndexDefinition edgeIndexDef1 = secondSchema.getEdgeIndexes().get(0);
            assertEquals("mixed-edge-index1", edgeIndexDef1.getName());
            assertEquals(1, edgeIndexDef1.getPropertyKeys().size());
            assertTrue(edgeIndexDef1.getPropertyKeys().contains("name"));

            IndexDefinition edgeIndexDef2 = secondSchema.getEdgeIndexes().get(1);
            assertEquals("mixed-edge-index2", edgeIndexDef2.getName());
            assertEquals(1, edgeIndexDef2.getPropertyKeys().size());
            assertTrue(edgeIndexDef2.getPropertyKeys().contains("name"));

            IndexDefinition edgeIndexDef3 = secondSchema.getEdgeIndexes().get(2);
            assertEquals("composite-edge-index2", edgeIndexDef3.getName());
            assertEquals(2, edgeIndexDef3.getPropertyKeys().size());
            assertTrue(edgeIndexDef3.getPropertyKeys().contains("name"));
            assertTrue(edgeIndexDef3.getPropertyKeys().contains("age"));

            IndexDefinition edgeIndexDef4 = secondSchema.getEdgeIndexes().get(3);
            assertEquals("composite-edge-index1", edgeIndexDef4.getName());
            assertEquals(2, edgeIndexDef4.getPropertyKeys().size());
            assertTrue(edgeIndexDef4.getPropertyKeys().contains("name"));
            assertTrue(edgeIndexDef4.getPropertyKeys().contains("age"));
        }

    }


    @Test
    public void testAddNewAndExistingPropertyKeysToMixedIndex() {

        int maxBatchSize = 1;
        Map<String, AtlasPropertyKey> existingPropertyKeys = makeTestMap("name","age");
        Map<String, AtlasPropertyKey> newPropertyKeys = makeTestMap("ssn","dob");
        Map<String,Index> pendingIndices = new HashMap<>();
        addMixedVertexIndex(pendingIndices, "mixed-vertex-index", existingPropertyKeys.get("name"), existingPropertyKeys.get("age"), newPropertyKeys.get("ssn"), newPropertyKeys.get("dob"));
        addMixedEdgeIndex(pendingIndices, "mixed-edge-index", existingPropertyKeys.get("name"), existingPropertyKeys.get("age"), newPropertyKeys.get("ssn"), newPropertyKeys.get("dob"));

        SchemaSplitter splitter = new SchemaSplitter(maxBatchSize, newPropertyKeys, pendingIndices, new HashSet<>(Arrays.asList("name","age")));
        List<Schema> batches = splitter.createSchemaUpdateBatches();
        assertEquals(2, batches.size());
        Schema firstSchema = batches.get(0);

        assertEquals(1, firstSchema.getVertexIndexes().size());
        assertEquals(1, firstSchema.getEdgeIndexes().size());
        IndexDefinition vertexIndexDef = firstSchema.getVertexIndexes().get(0);
        assertEquals("mixed-vertex-index", vertexIndexDef.getName());
        assertEquals(1, firstSchema.getPropertyKeys().size());
        assertEquals("dob", firstSchema.getPropertyKeys().iterator().next().getName());
        assertEquals(3, vertexIndexDef.getPropertyKeys().size());
        assertTrue(vertexIndexDef.getPropertyKeys().contains("name"));
        assertTrue(vertexIndexDef.getPropertyKeys().contains("age"));
        assertTrue(vertexIndexDef.getPropertyKeys().contains("dob"));

        IndexDefinition edgeIndexDef = firstSchema.getEdgeIndexes().get(0);
        assertEquals("mixed-edge-index", edgeIndexDef.getName());
        assertEquals(3, edgeIndexDef.getPropertyKeys().size());
        assertTrue(edgeIndexDef.getPropertyKeys().contains("name"));
        assertTrue(edgeIndexDef.getPropertyKeys().contains("age"));
        assertTrue(edgeIndexDef.getPropertyKeys().contains("dob"));

        //there are two property keys in play here, so those get split into two batches, one for
        //each property key.  The processing of the second property key should discover that there
        //is nothing to do and create an empty schema.
        Schema secondSchema = batches.get(1);
        assertEquals(1, secondSchema.getVertexIndexes().size());
        assertEquals(1, secondSchema.getEdgeIndexes().size());
        IndexDefinition vertexIndexDef2 = secondSchema.getVertexIndexes().get(0);
        assertEquals("mixed-vertex-index", vertexIndexDef2.getName());
        assertEquals(1, secondSchema.getPropertyKeys().size());
        assertEquals("ssn", secondSchema.getPropertyKeys().iterator().next().getName());
        assertEquals(1, vertexIndexDef2.getPropertyKeys().size());
        assertTrue(vertexIndexDef2.getPropertyKeys().contains("ssn"));


        IndexDefinition edgeIndexDef2 = secondSchema.getEdgeIndexes().get(0);
        assertEquals("mixed-edge-index", edgeIndexDef2.getName());
        assertEquals(1, edgeIndexDef2.getPropertyKeys().size());
        assertTrue(edgeIndexDef2.getPropertyKeys().contains("ssn"));

    }


    @Test
    public void testAddNewAndExistingPropertyKeysToCompositeIndex() {

        int maxBatchSize = 1;
        Map<String, AtlasPropertyKey> existingPropertyKeys = makeTestMap("name","age");
        Map<String, AtlasPropertyKey> newPropertyKeys = makeTestMap("ssn","dob");
        Map<String,Index> pendingIndices = new HashMap<>();
        addCompositeVertexIndex(pendingIndices, "composite-vertex-index", existingPropertyKeys.get("name"), existingPropertyKeys.get("age"), newPropertyKeys.get("ssn"), newPropertyKeys.get("dob"));
        addCompositeEdgeIndex(pendingIndices, "composite-edge-index", existingPropertyKeys.get("name"), existingPropertyKeys.get("age"), newPropertyKeys.get("ssn"), newPropertyKeys.get("dob"));

        SchemaSplitter splitter = new SchemaSplitter(maxBatchSize, newPropertyKeys, pendingIndices, new HashSet<>(Arrays.asList("name","age")));
        List<Schema> batches = splitter.createSchemaUpdateBatches();
        assertEquals(2, batches.size());
        Schema firstSchema = batches.get(0);

        assertEquals(0, firstSchema.getVertexIndexes().size());
        assertEquals(0, firstSchema.getEdgeIndexes().size());
        assertEquals(1, firstSchema.getPropertyKeys().size());
        assertEquals("dob", firstSchema.getPropertyKeys().iterator().next().getName());

        //there are two property keys in play here, so those get split into two batches, one for
        //each property key.  The processing of the second property key should discover that there
        //is nothing to do and create an empty schema.
        Schema secondSchema = batches.get(1);
        assertEquals(1, secondSchema.getVertexIndexes().size());
        assertEquals(1, secondSchema.getEdgeIndexes().size());
        IndexDefinition vertexIndexDef2 = secondSchema.getVertexIndexes().get(0);
        assertEquals("composite-vertex-index", vertexIndexDef2.getName());
        assertEquals(4, vertexIndexDef2.getPropertyKeys().size());
        assertTrue(vertexIndexDef2.getPropertyKeys().contains("ssn"));
        assertTrue(vertexIndexDef2.getPropertyKeys().contains("dob"));
        assertTrue(vertexIndexDef2.getPropertyKeys().contains("name"));
        assertTrue(vertexIndexDef2.getPropertyKeys().contains("age"));

        IndexDefinition edgeIndexDef2 = secondSchema.getEdgeIndexes().get(0);
        assertEquals("composite-edge-index", edgeIndexDef2.getName());
        assertEquals(4, edgeIndexDef2.getPropertyKeys().size());
        assertTrue(edgeIndexDef2.getPropertyKeys().contains("ssn"));
        assertTrue(edgeIndexDef2.getPropertyKeys().contains("dob"));
        assertTrue(edgeIndexDef2.getPropertyKeys().contains("name"));
        assertTrue(edgeIndexDef2.getPropertyKeys().contains("age"));

        assertEquals(1, secondSchema.getPropertyKeys().size());
        assertEquals("ssn", secondSchema.getPropertyKeys().iterator().next().getName());
    }



    protected void addCompositeVertexIndex(Map<String,Index> toUpdate, String name, AtlasPropertyKey...keys) {
        Index idx = new Index(name, true, false, false, IndexType.VERTEX, GraphDBUtil.unwrapPropertyKeys(Arrays.asList(keys)));
        toUpdate.put(name, idx);
    }

    protected void addCompositeEdgeIndex(Map<String,Index> toUpdate, String name, AtlasPropertyKey...keys) {
        Index idx = new Index(name, true, false, false, IndexType.EDGE, GraphDBUtil.unwrapPropertyKeys(Arrays.asList(keys)));
        toUpdate.put(name, idx);
    }

    protected void addMixedEdgeIndex(Map<String,Index> toUpdate, String name, AtlasPropertyKey...keys) {
        Index idx = new Index(name, false, false, false, IndexType.EDGE, GraphDBUtil.unwrapPropertyKeys(Arrays.asList(keys)));
        toUpdate.put(name, idx);
    }

    protected void addMixedVertexIndex(Map<String,Index> toUpdate, String name, AtlasPropertyKey...keys) {
        Index idx = new Index(name, false, false, false, IndexType.VERTEX, GraphDBUtil.unwrapPropertyKeys(Arrays.asList(keys)));
        toUpdate.put(name, idx);
    }


    protected Map<String, AtlasPropertyKey> makeTestMap(String... propertyKeyNames) {
        Map<String,AtlasPropertyKey> result = new HashMap<>(propertyKeyNames.length);
        for(String name : propertyKeyNames) {
            PropertyKey toWrap = new PropertyKey(name, PropertyDataType.String, Cardinality.SINGLE);
            AtlasPropertyKey key = new IBMGraphPropertyKey(toWrap);
            result.put(name, key);
        }
       return result;
    }
}
