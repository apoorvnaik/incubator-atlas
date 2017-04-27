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

import org.apache.atlas.AtlasException;
import org.apache.atlas.ibmgraph.IBMGraphDatabase;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;
import org.testng.Assert;
/**
 * Test class to test creating sessions in multiple threads.
 * @author
 *
 */
public class SessionCreateTest {
    protected void runInParallelInNewThreads(Runnable r, int threadCount) throws Throwable {

        Thread[] threads = new Thread[threadCount];
        for(int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(r);
        }

        //try to start the threads at the same time
        for(int i = 0; i < threadCount; i++) {
            threads[i].start();
        }

        //wait for the threads to finish

        for(Thread th : threads) {
            th.join();
        }

    }
    @Test
    public void testMultipeSessionCreateRequests () throws Throwable {
        Runnable r = new Runnable() {
            @Override
            public void run() {
                getGraph();

            }
        };

        runInParallelInNewThreads(r, 3);
    }
    private final <V, E> AtlasGraph<V, E> getGraph() {
        IBMGraphDatabase db = new IBMGraphDatabase();
        db.initializeTestGraph();
        return (AtlasGraph<V, E>) db.getUserGraph();
    }

    @AfterClass
    public static void cleanUp() throws AtlasException {
        IBMGraphDatabase db = new IBMGraphDatabase();
        db.cleanup(true);
    }
}
