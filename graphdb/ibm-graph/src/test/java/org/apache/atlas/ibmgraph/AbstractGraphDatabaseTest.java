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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import org.apache.atlas.AtlasException;
import org.apache.atlas.ibmgraph.IBMGraphDatabase;
import org.apache.atlas.ibmgraph.IBMGraphGraph;
import org.apache.atlas.ibmgraph.api.GraphDatabaseConfiguration;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasCardinality;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 *
 */
public abstract class AbstractGraphDatabaseTest {

    protected static final String SIZE_PROPERTY = "size15";
    protected static final String NAME_PROPERTY = "name";
    //use a new graph when the tests are run
    private static final String GRAPH_ID = "test-" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();
    static {
        GraphDatabaseConfiguration.INSTANCE.setGraphName(GRAPH_ID);
    }
    private static final String TENANT_ID = "test-" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();
    static {
        //set this now so that the indexing in oneTimeSetup picks it up
        IBMGraphDatabase.setTenantId(TENANT_ID);
    }

    private static class RunnableWrapper implements Runnable {
        private final Runnable r;
        private Throwable exceptionThrown_ = null;


        private RunnableWrapper(Runnable r) {
            this.r = r;
        }

        @Override
        public void run() {
            String name = Thread.currentThread().getName();
            try {
                System.err.println("Thread '" + name  + "' started.");
                r.run();
                System.err.println("Thread '" + name  + "' completed successfully.");
            }
            catch(Throwable e) {
                System.err.println("Thread '" + name + "' completed with an error:");
                e.printStackTrace();
                exceptionThrown_ = e;
            }

        }

        public Throwable getExceptionThrown() {
            return exceptionThrown_;
        }
    }

    protected static final String WEIGHT_PROPERTY = "weight";
    protected static final String TRAIT_NAMES = Constants.TRAIT_NAMES_PROPERTY_KEY;
    protected static final String typeProperty = "__type";
    protected static final String typeSystem = "typeSystem";

    private static final String BACKING_INDEX_NAME = "backing";

    private AtlasGraph<?,?> graph = null;

    @AfterMethod
    public void commitGraph() throws AtlasException {
        //force any pending actions to be committed so we can be sure they don't cause errors.
        pushChangesAndFlushCache();
        graph.commit();
        graph.clear();
    }

    protected <V, E> void pushChangesAndFlushCache() {
        AtlasGraph<V, E> graph = getGraph();
        graph.commit();
        ((IBMGraphGraph)graph).flushCache();
    }


    @BeforeClass
    public static void createIndices() throws AtlasException {

        IBMGraphDatabase db = new IBMGraphDatabase();
        AtlasGraphManagement mgmt = db.getSharedGraph().getManagementSystem();

        if(mgmt.getGraphIndex(BACKING_INDEX_NAME) == null) {
            mgmt.createVertexIndex(BACKING_INDEX_NAME, Constants.BACKING_INDEX, Collections.emptyList());
        }
        AtlasPropertyKey key = mgmt.makePropertyKey("age13",Integer.class, AtlasCardinality.SINGLE);
        if(mgmt.getGraphIndex("testindex") == null) {
            mgmt.createExactMatchIndex("testindex", false, Collections.singletonList(key));
        }
        createIndices(mgmt, NAME_PROPERTY, String.class, false, AtlasCardinality.SINGLE);
        createIndices(mgmt, WEIGHT_PROPERTY, Integer.class, false, AtlasCardinality.SINGLE);
        createIndices(mgmt, SIZE_PROPERTY, String.class, false, AtlasCardinality.SINGLE);
        createIndices(mgmt, "typeName", String.class, false, AtlasCardinality.SINGLE);
        createIndices(mgmt, "__type", String.class, false, AtlasCardinality.SINGLE);
        createIndices(mgmt, Constants.GUID_PROPERTY_KEY, String.class, true, AtlasCardinality.SINGLE);
        createIndices(mgmt, Constants.TRAIT_NAMES_PROPERTY_KEY, String.class, false, AtlasCardinality.SET);
        createIndices(mgmt, Constants.SUPER_TYPES_PROPERTY_KEY, String.class, false, AtlasCardinality.SET);
        mgmt.commit();
    }

    @AfterClass
    public static void cleanUp() throws AtlasException {

        IBMGraphDatabase db = new IBMGraphDatabase();
        db.cleanup(true);
    }

    private static void createIndices(AtlasGraphManagement management, String propertyName, Class propertyClass,
                                      boolean isUnique, AtlasCardinality cardinality) {

        if(management.containsPropertyKey(propertyName)) {
            //index was already created
            return;
        }

        AtlasPropertyKey key = management.makePropertyKey(propertyName, propertyClass, cardinality);
        try {
            if(propertyClass != Integer.class) {
                management.addVertexIndexKey(BACKING_INDEX_NAME, key);
            }
        } catch(Throwable t) {
            //ok
            t.printStackTrace();
        }
        try {
            management.createExactMatchIndex(propertyName,isUnique, Collections.singletonList(key));

        } catch(Throwable t) {
            //ok
            t.printStackTrace();
        }


    }


    /**
     *
     */
    public AbstractGraphDatabaseTest() {
        super();
    }


    protected final <V, E> AtlasGraph<V, E> getGraph() {
        if (graph == null) {
            IBMGraphDatabase db = new IBMGraphDatabase();
            //set the tenant id to the tenant we're using -- this is needed
            //for multi-threaded tests
            IBMGraphDatabase.setTenantId(TENANT_ID);
            graph = db.getUserGraph();
        }
        return (AtlasGraph<V, E>) graph;
    }

    @AfterMethod
    public final <V, E> void cleanupVertices() {
        AtlasGraph<V,E> graph = getGraph();
        graph.clear();
    }


    protected final <V, E> AtlasVertex<V, E> createVertex(AtlasGraph<V, E> graph) {
        AtlasVertex<V,E> vertex = graph.addVertex();
        return vertex;
    }

    protected void runSynchronouslyInNewThread(final Runnable r) throws Throwable {

        RunnableWrapper wrapper = new RunnableWrapper(r);
        Thread th = new Thread(wrapper);
        th.start();
        th.join();
        Throwable ex = wrapper.getExceptionThrown();
        if(ex != null) {
            throw ex;
        }
    }

    protected void runInParallelInNewThreads(Runnable r, int threadCount) throws Throwable {

        Thread[] threads = new Thread[threadCount];
        RunnableWrapper[] wrappers = new RunnableWrapper[threadCount];
        for(int i = 0; i < threadCount; i++) {
            RunnableWrapper wrapper = new RunnableWrapper(r);
            Thread th = new Thread(wrapper);
            threads[i] = th;
            wrappers[i] = wrapper;
        }

        //try to start the threads at the same time
        for(int i = 0; i < threadCount; i++) {
            threads[i].start();
        }

        //wait for the threads to finish

        for(Thread th : threads) {
            th.join();
        }

        //check to see if there were any issues
        Collection<Throwable> errors = new ArrayList<Throwable>();
        for(RunnableWrapper wrapper: wrappers) {
            Throwable ex = wrapper.getExceptionThrown();
            if(ex != null) {
                errors.add(ex);
            }
        }
        if(errors.size() > 0) {
            throw join(errors);
        }
    }

    protected IBMGraphGraph getIbmGraphGraph() {
        AtlasGraph g = getGraph();
        return (IBMGraphGraph)g;
    }

    private Throwable join(Collection<Throwable> exceptions) {
        StringBuilder builder = new StringBuilder();

        builder.append(exceptions.size() + " Exceptions occurred: ");

        Iterator<Throwable> it = exceptions.iterator();
        while(it.hasNext()) {
            Throwable next = it.next();

            builder.append(next.getClass().getName());
            if(next.getMessage() != null) {
                builder.append(next.getMessage());
            }
            if(it.hasNext()) {
                builder.append(",");
            }
        }
        builder.append("\nStack Traces:\n\n");
        it = exceptions.iterator();
        while(it.hasNext()) {
            Throwable next = it.next();
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            next.printStackTrace(new PrintStream(os));
            builder.append(os.toString());
            builder.append("\n");
        }
        return new AssertionError(builder.toString());
    }
}
