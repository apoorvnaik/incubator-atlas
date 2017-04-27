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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.atlas.ibmgraph.util.Batcher;
import org.testng.annotations.Test;



/**
 * Tests for Batcher
 */
public class BatcherTest {

    @Test
    public void testNegativeBatchSize() {

        List<Integer> toBatch = Arrays.asList(1,2,3);
        Batcher<Integer> batcher = new Batcher<>(toBatch, -1);
        assertTrue(batcher.hasNext());
        List<Integer> batch1 = batcher.next();
        assertEquals(3, batch1.size());
        assertEquals(1, batch1.get(0).intValue());
        assertEquals(2, batch1.get(1).intValue());
        assertEquals(3, batch1.get(2).intValue());
        assertFalse(batcher.hasNext());
    }

    @Test
    public void testNothingToBatch() {

        List<Integer> toBatch = Collections.emptyList();
        Batcher<Integer> batcher = new Batcher<>(toBatch, 3);
        assertFalse(batcher.hasNext());
    }

    @Test
    public void testOneBatch() {
        List<Integer> toBatch = Arrays.asList(1,2,3);
        Batcher<Integer> batcher = new Batcher<>(toBatch, 10);
        assertTrue(batcher.hasNext());
        List<Integer> batch1 = batcher.next();
        assertEquals(3, batch1.size());
        assertEquals(1, batch1.get(0).intValue());
        assertEquals(2, batch1.get(1).intValue());
        assertEquals(3, batch1.get(2).intValue());
        assertFalse(batcher.hasNext());
    }

    @Test
    public void testBatcher() {

        List<Integer> toBatch = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        Batcher<Integer> batcher = new Batcher<>(toBatch, 3);
        assertTrue(batcher.hasNext());

        List<Integer> batch1 = batcher.next();
        assertEquals(3, batch1.size());
        assertEquals(1, batch1.get(0).intValue());
        assertEquals(2, batch1.get(1).intValue());
        assertEquals(3, batch1.get(2).intValue());

        assertTrue(batcher.hasNext());
        List<Integer> batch2 = batcher.next();
        assertEquals(3, batch2.size());
        assertEquals(4, batch2.get(0).intValue());
        assertEquals(5, batch2.get(1).intValue());
        assertEquals(6, batch2.get(2).intValue());

        assertTrue(batcher.hasNext());
        List<Integer> batch3 = batcher.next();
        assertEquals(3, batch3.size());
        assertEquals(7, batch3.get(0).intValue());
        assertEquals(8, batch3.get(1).intValue());
        assertEquals(9, batch3.get(2).intValue());

        assertTrue(batcher.hasNext());
        List<Integer> batch4 = batcher.next();
        assertEquals(1, batch4.size());
        assertEquals(10, batch4.get(0).intValue());
        assertFalse(batcher.hasNext());
    }
}
