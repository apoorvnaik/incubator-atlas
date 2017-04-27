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

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.groovy.FunctionCallExpression;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.GroovyGenerationContext;
import org.apache.atlas.ibmgraph.gremlin.expr.TransformQueryResultExpression;
import org.apache.atlas.ibmgraph.gremlin.stmt.PreGeneratedStatement;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.script.ScriptException;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 *
 */
public class GraphDatabaseQueryTest extends AbstractGraphDatabaseTest {

    private ObjectInfo info;


    @BeforeMethod
    public <V, E> void createObjectInfo() {
        AtlasGraph<V, E> graph = getGraph();
        graph.clear();
        info = null;
        info = new ObjectInfo<>();
    }


    @Test
    public <V, E >void testNormalVertexQuery() throws ScriptException, AtlasBaseException {

        List list = executeGremlinScriptOnGraph("g.V().has('size15', 'small')", false, false);
        List convertedValues = list;

        assertTrue(convertedValues.contains(info.getGeorgeVertex()));
        assertTrue(convertedValues.contains(info.getRonVertex()));
        assertFalse(convertedValues.contains(info.getFredVertex()));
    }

    @Test
    public <V, E >void testNormalVertexQuery2() throws ScriptException, AtlasBaseException {

        String query = "g.V().has('size15', 'big')";
        List list = executeGremlinScriptOnGraph(query, false, false);
        List convertedValues = list;

        assertFalse(convertedValues.contains(info.getGeorgeVertex()));
        assertFalse(convertedValues.contains(info.getRonVertex()));
        assertTrue(convertedValues.contains(info.getFredVertex()));
    }

    @Test
    public <V, E >void testVertexQueryWithPath() throws ScriptException, AtlasBaseException {

        String query = String.format("g.V(%1s).out('knows').path()", info.getFredVertex().getId().toString());
        List list = executeGremlinScriptOnGraph(query, false, true);
        List pathResult = (List)list.get(0);
        List convertedValues = (List)pathResult;

        assertEquals(2, convertedValues.size());
        assertEquals(info.getFredVertex(), convertedValues.get(0));
        assertEquals(info.getGeorgeVertex(), convertedValues.get(1));
    }



    @Test
    public <V, E >void testSelectQueryWithOneOutput() throws ScriptException, AtlasBaseException {


        List list = executeGremlinScriptOnGraph("g.V().has('size15', 'small').as('a').select('a').by({[(it as Vertex).value('name15'), (it as Vertex).value('age15')]} as Function)", true, false);

        for(Object rawValue : list) {
            Object name = getColumnValue(rawValue, "a", 0);
            Object age = getColumnValue(rawValue, "a", 1);
            assertTrue(name.equals("George") && age.equals("13") ||
                    name.equals("Ron") && age.equals("11"));
        }
    }

    @Test
    public <V, E >void testSelectQueryWithMultipleOutputs() throws ScriptException, AtlasBaseException {

        List list = executeGremlinScriptOnGraph("g.V().has('size15', 'small').as('a').in('knows').as('b').select('a', 'b').by({[(it as Vertex).value('name15'), (it as Vertex).value('age15')]} as Function).by({[(it as Vertex).value('name15')]} as Function)", true, false);
        for(Object rawValue : list) {
            Object knowerName = getColumnValue(rawValue, "a", 0);
            Object knowerAge = getColumnValue(rawValue, "a", 1);
            Object knowee = getColumnValue(rawValue, "b", 0);
            assertEquals("George", knowerName);
            assertEquals("13", knowerAge);
            assertEquals("Fred", knowee);
        }

    }

    @Test
    public <V, E >void testQueryWithArithmeticOperator() throws ScriptException, AtlasBaseException {

        List list = executeGremlinScriptOnGraph("g.V().has('size15', 'small').in('knows').as('b').select('b').by({[(it as Vertex).<Long>value('age13')+5]} as Function)", true, false);
        for(Object rawValue : list) {
            Object enhancedAge = getColumnValue(rawValue, "b", 0);
            assertEquals(18, ((Number)enhancedAge).intValue());
        }
    }

    @Test
    public <V, E >void testSingleColumnSelectWithPath() throws ScriptException, AtlasBaseException {

        List list = executeGremlinScriptOnGraph("g.V().has('size15', 'small').in('knows').as('b').select('b').by({[(it as Vertex).value('name15'), (it as Vertex).value('age15')]} as Function).path()", true, true);

        for(Object rawValue : list) {
            List pathResult = (List)rawValue;
            Object vertex1 = pathResult.get(0);
            Object vertex2 = pathResult.get(1);
            Object selectOutput = pathResult.get(2);

            assertEquals(info.getGeorgeVertex(), vertex1);
            assertEquals(info.getFredVertex(), vertex2);
            Object name = getColumnValue(selectOutput, "b", 0);
            Object age = getColumnValue(selectOutput, "b", 1);

            assertEquals("Fred", name);
            assertEquals("13", age);
        }
    }

    public <V, E >void testMultipleColumnSelectWithPath() throws ScriptException, AtlasBaseException {

        List list = executeGremlinScriptOnGraph("g.V().has('size15', 'small').as('a').in('knows').as('b').select('a', 'b').by({[(it as Vertex).value('name15'), (it as Vertex).value('age15')]} as Function).by({[(it as Vertex).value('name15')]} as Function).path()", true, true);

        for(Object rawValue : list) {
            List pathResult = (List)rawValue;
            Object vertex1 = pathResult.get(0);
            Object vertex2 = pathResult.get(1);
            Object selectOutput = pathResult.get(2);

            assertEquals(info.getGeorgeVertex(), vertex1);
            assertEquals(info.getFredVertex(), vertex2);
            Object knowerName = getColumnValue(selectOutput, "a", 0);
            Object knowerAge = getColumnValue(selectOutput, "a", 1);
            Object knowee = getColumnValue(selectOutput, "b", 0);
            assertEquals("George", knowerName);
            assertEquals("13", knowerAge);
            assertEquals("Fred", knowee);
        }
    }

    @Test
    public <V, E >void testMultipleColumnSelectWithPathWithTypeKeyAndMultipleOutputs() throws ScriptException, AtlasBaseException {

        List list = executeGremlinScriptOnGraph("g.V().has('size15', 'small').as('type').in('knows').as('b').select('type', 'b').by({[(it as Vertex).value('name15'), (it as Vertex).value('age15')]} as Function).by({[(it as Vertex).value('name15')]} as Function).path()", true, true);

        for(Object rawValue : list) {
            List pathResult = (List)rawValue;
            Object vertex1 = pathResult.get(0);
            Object vertex2 = pathResult.get(1);
            Object selectOutput = pathResult.get(2);

            assertEquals(info.getGeorgeVertex(), vertex1);
            assertEquals(info.getFredVertex(), vertex2);
            Object knowerName = getColumnValue(selectOutput, "type", 0);
            Object knowerAge = getColumnValue(selectOutput, "type", 1);
            Object knowee = getColumnValue(selectOutput, "b", 0);
            assertEquals("George", knowerName);
            assertEquals("13", knowerAge);
            assertEquals("Fred", knowee);
        }
    }
    @Test
    public <V, E >void testMultipleColumnSelectWithPathWithTypeKeyAndOneOutput() throws ScriptException, AtlasBaseException {

        List list = executeGremlinScriptOnGraph("g.V().has('size15', 'small').as('type').in('knows').as('b').select('type', 'b').by({[(it as Vertex).value('age15')]} as Function).by({[(it as Vertex).value('name15')]} as Function).path()", true, true);

        for(Object rawValue : list) {
            List pathResult = (List)rawValue;
            Object vertex1 = pathResult.get(0);
            Object vertex2 = pathResult.get(1);
            Object selectOutput = pathResult.get(2);

            assertEquals(info.getGeorgeVertex(), vertex1);
            assertEquals(info.getFredVertex(), vertex2);
            Object knowerAge = getColumnValue(selectOutput, "type", 0);
            Object knowee = getColumnValue(selectOutput, "b", 0);
            assertEquals("13", knowerAge);
            assertEquals("Fred", knowee);
        }
    }

    @Test
    public <V, E> void testMultipleSelectQueryThatReturnsVertices() throws Exception {

        List list = executeGremlinScriptOnGraph("g.V().has('size15', 'small').as('a').in('knows').as('b').select('a', 'b').by({[(it as Vertex), (it as Vertex).value('name15'), (it as Vertex).value('age15')]} as Function).by({[(it as Vertex).value('name15')]} as Function)", true, false);
        for(Object rawValue : list) {
            AtlasVertex knowerVertex = (AtlasVertex)getColumnValue(rawValue, "a", 0);

            Object knowerName = getColumnValue(rawValue, "a", 1);
            Object knowerAge = getColumnValue(rawValue, "a", 2);
            Object knowee = getColumnValue(rawValue, "b", 0);
            assertEquals("George", knowerVertex.getProperty("name15", String.class));
            assertEquals("13", knowerVertex.getProperty("age15", String.class));
            assertEquals("George", knowerName);
            assertEquals("13", knowerAge);
            assertEquals("Fred", knowee);
        }

    }

    @Test
    public <V, E> void testSingleSelectQueryThatReturnsVertices() throws Exception {

        List list = executeGremlinScriptOnGraph("g.V().has('size15', 'small').as('a').in('knows').select('a').by({[(it as Vertex), (it as Vertex).value('name15'), (it as Vertex).value('age15')]} as Function)", true, false);
        for(Object rawValue : list) {
            AtlasVertex knowerVertex = (AtlasVertex)getColumnValue(rawValue, "a", 0);

            Object knowerName = getColumnValue(rawValue, "a", 1);
            Object knowerAge = getColumnValue(rawValue, "a", 2);
            assertEquals("George", knowerVertex.getProperty("name15", String.class));
            assertEquals("13", knowerVertex.getProperty("age15", String.class));
            assertEquals("George", knowerName);
            assertEquals("13", knowerAge);
        }

    }

    private <V, E> List executeGremlinScriptOnGraph(String query, boolean isSelect, boolean isPath) throws ScriptException, AtlasBaseException {
        GroovyExpression queryExpression = new PreGeneratedStatement(query);
        GroovyExpression updatedQuery = new FunctionCallExpression(queryExpression, "toList");
        updatedQuery = addQuerySuffix(updatedQuery, isSelect, isPath);
        GroovyGenerationContext context = new GroovyGenerationContext();
        context.setParametersAllowed(false);
        updatedQuery.generateGroovy(context);
        String queryString =context.getQuery();

        AtlasGraph<V, E> graph = (AtlasGraph<V, E>)getGraph();
        return (List)graph.executeGremlinScript(queryString,  isPath);
    }

    private class ObjectInfo<V, E> {

        private AtlasVertex<V, E> fredVertex_;
        private AtlasVertex<V, E> georgeVertex_;
        private AtlasVertex<V, E> ronVertex_;

        public ObjectInfo() {

            final AtlasGraph<V, E> graph = (AtlasGraph<V, E>)getGraph();

            fredVertex_ = createVertex(graph);
            fredVertex_.addProperty("size15", "big");
            fredVertex_.addProperty("name15", "Fred");
            fredVertex_.addProperty("age15", "13");
            fredVertex_.addProperty("age13", 13);

            georgeVertex_ = createVertex(graph);
            georgeVertex_.addProperty("size15", "small");
            georgeVertex_.addProperty("name15", "George");
            georgeVertex_.addProperty("age15", "13");
            georgeVertex_.addProperty("age13", 13);

            ronVertex_ = createVertex(graph);
            ronVertex_.addProperty("size15", "small");
            ronVertex_.addProperty("name15", "Ron");
            ronVertex_.addProperty("age15", "11");
            ronVertex_.addProperty("age13", 11);

            graph.addEdge(fredVertex_, georgeVertex_, "knows");
            graph.commit();

        }

        /**
         * @return the fredVertex
         */
        public AtlasVertex<V, E> getFredVertex() {
            return fredVertex_;
        }

        /**
         * @return the georgeVertex_
         */
        public AtlasVertex<V, E> getGeorgeVertex() {
            return georgeVertex_;
        }

        /**
         * @return the ronVertex
         */
        public AtlasVertex<V, E> getRonVertex() {
            return ronVertex_;
        }


    }

    private GroovyExpression addQuerySuffix(GroovyExpression expr, boolean isSelect, boolean isPath) {
        TransformQueryResultExpression result = new TransformQueryResultExpression(expr, isSelect, isPath);
        return result;
    }

    private Object getColumnValue(Object rowValue, String colName, int idx) {

            Object rawColumnValue = null;
            if(rowValue instanceof Map) {
                Map columnsMap = (Map)rowValue;
                rawColumnValue = columnsMap.get(colName);
            }
            else {
                //when there is only one column, result does not come back as a map
                rawColumnValue = rowValue;
            }

            Object value = null;
            if(rawColumnValue instanceof List && idx >= 0) {
                List arr = (List)rawColumnValue;
                value = arr.get(idx);
            }
            else {
                value = rawColumnValue;
            }

            return value;
        }
}
