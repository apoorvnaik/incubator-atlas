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
import static org.testng.Assert.fail;

import org.apache.atlas.AtlasException;
import org.apache.atlas.ibmgraph.IBMGraphDatabase;
import org.apache.atlas.ibmgraph.exception.GraphDatabaseException;
import org.apache.atlas.repository.graphdb.AtlasCardinality;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.slf4j.LoggerFactory;

//we can't extend GraphDatabaseTest because that will cause the default log4j configuration
//to be used (before this class is even loaded), defeating the purpose of this test.
public class UpdateScriptLoggingOffTest  {

    private AtlasGraph<?,?> graph = null;

    //this test must be run in a forked jvm
    static {
        System.setProperty("log4j.configuration","update-logging-off-log4j.xml");
    }

    @BeforeClass
    public void setUp() throws AtlasException {
        IBMGraphDatabase db = new IBMGraphDatabase();
        db.initializeTestGraph();
    }

    @AfterClass
    public void tearDown() throws AtlasException {
        IBMGraphDatabase db = new IBMGraphDatabase();
        db.cleanup(true);
    }

    //run this first so the index will be created quickly.  Otherwise,
    //a reindex is needed...
    @Test
    public <V,E> void testError() {
        AtlasGraph<V,E> graph = (AtlasGraph<V,E>)getGraph();
        AtlasGraphManagement mgmt = graph.getManagementSystem();
        AtlasPropertyKey key = mgmt.getPropertyKey("floatAttr");
        if(key == null) {
            key = mgmt.makePropertyKey("floatAttr", Float.class, AtlasCardinality.SINGLE);
        }
        mgmt.commit();
        AtlasVertex<V,E> v1 = graph.addVertex();
        v1.setProperty("floatAttr", "Hello World!");
        try {
            graph.commit();
            fail("Expected exception not thrown");
        }
        catch(GraphDatabaseException expected) {

        }

    }


    @Test(dependsOnMethods="testError")
    public <V,E> void testUpdateLoggingDisabled() {
        assertEquals(false, LoggerFactory.getLogger("GraphUpdateScriptLogger").isDebugEnabled());
    }

    @Test(dependsOnMethods="testError")
    public <V,E> void testAddVertex() {
        AtlasGraph<V,E> graph = (AtlasGraph<V,E>)getGraph();
        AtlasVertex<V,E> v1 = graph.addVertex();
        graph.commit();

    }


    protected final <V, E> AtlasGraph<V, E> getGraph() {
        if (graph == null) {
            IBMGraphDatabase db = new IBMGraphDatabase();
            graph = db.getUserGraph();
        }
        return (AtlasGraph<V, E>) graph;
    }
}
