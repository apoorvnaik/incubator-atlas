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

import java.util.Collections;
import java.util.List;

import org.apache.atlas.ibmgraph.AbstractGraphDatabaseTest;
import org.apache.atlas.ibmgraph.IBMGraphVertex;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.commons.collections.IteratorUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PropertyValueToVerticesMapTest extends AbstractGraphDatabaseTest {

    static enum TestEnum {
        A,
        B,
        C
    }

    @Test
    public void testEnumHandling() {


        IBMGraphVertex v1 = getIbmGraphGraph().addVertex().getV();
        IBMGraphVertex v2 = getIbmGraphGraph().addVertex().getV();

        PropertyValueToVerticesMap map = new PropertyValueToVerticesMap();
        map.recordPropertySet(v1, TestEnum.A);
        map.recordPropertySet(v2,  TestEnum.B);

        Assert.assertEquals(map.getElements(TestEnum.A), Collections.singleton(v1));

        Assert.assertEquals(map.getElements(TestEnum.B), Collections.singleton(v2));

        map.recordPropertyRemove(v1, TestEnum.A);
        Assert.assertTrue(map.getElements(TestEnum.A).isEmpty());

    }

    @Test
    public void testEnumIndexing() {

        AtlasVertex v1 = getGraph().addVertex();
        AtlasVertex v2 = getGraph().addVertex();

        v1.setProperty(NAME_PROPERTY, "Fred");
        v1.setProperty(SIZE_PROPERTY, TestEnum.A);

        v2.setProperty(NAME_PROPERTY, "George");
        v2.setProperty(SIZE_PROPERTY, TestEnum.A);

        List vertices = IteratorUtils.toList(getGraph().query().has(SIZE_PROPERTY, TestEnum.A).vertices().iterator());
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v2));


        pushChangesAndFlushCache();

        String v1Id = v1.getId().toString();

        AtlasVertex v1Loaded = getGraph().getVertex(v1Id);
        //this causes the size property to be added to the index.
        v1Loaded.setProperty(NAME_PROPERTY, "George");

        //make sure that the property map correctly handles the checking of he SIZE property in the
        //vertex that was loaded from the graph
        vertices = IteratorUtils.toList(getGraph().query().has(SIZE_PROPERTY, TestEnum.A).has(NAME_PROPERTY,"George").vertices().iterator());
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1Loaded));
    }
}
