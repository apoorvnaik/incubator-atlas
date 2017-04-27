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
import org.apache.atlas.ibmgraph.api.Cardinality;
import org.apache.atlas.ibmgraph.api.GraphDatabaseClient;
import org.apache.atlas.ibmgraph.api.GraphDatabaseConfiguration;
import org.apache.atlas.ibmgraph.api.json.PropertyDataType;
import org.apache.atlas.ibmgraph.api.json.PropertyKey;
import org.apache.atlas.ibmgraph.api.json.Schema;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.Test;


/**
 * Sanity test that verifies that IBM Graph does not complain if a property
 * is included in multiple schema update requests.
 */
public class PropertyDuplicationTest {

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
    @Test
    public <V,E> void testCreateSamePropertyKeyTwice() throws AtlasException {
        //this test validates that IBM Graph does not complain if we try to create the same
        //property key twice.

        Schema s = new Schema();
        s.addPropertyKey(new PropertyKey("duplicated",PropertyDataType.Integer, Cardinality.SINGLE));
        GraphDatabaseClient client = new GraphDatabaseClient(GRAPH_ID);
        try {
            client.createGraph();
            client.updateSchema(s, null);
            client.updateSchema(s, null);
        }
        finally {
            client.deleteGraph(GRAPH_ID);
        }
    }


}
