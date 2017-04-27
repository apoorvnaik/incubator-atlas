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

package org.apache.atlas.ibmgraph.gremlin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.atlas.groovy.FunctionCallExpression;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.IdentifierExpression;
import org.apache.atlas.groovy.LiteralExpression;
import org.apache.atlas.groovy.VariableAssignmentExpression;
import org.apache.atlas.ibmgraph.IBMGraphEdge;
import org.apache.atlas.ibmgraph.IBMGraphElement;
import org.apache.atlas.ibmgraph.IBMGraphGraph;
import org.apache.atlas.ibmgraph.IBMGraphVertex;
import org.apache.atlas.ibmgraph.TenantGraphStrategy;
import org.apache.atlas.ibmgraph.api.GraphDatabaseConfiguration;
import org.apache.atlas.ibmgraph.gremlin.expr.GetElementExpression;
import org.apache.atlas.ibmgraph.gremlin.expr.GetElementExpression.ElementType;


/**
 * Context used when translating actions into their corresponding Gremlin Groovy
 * expressions.  Keeps track of elements that were added and variables for
 * vertices and edges.
 *
 */
public class ActionTranslationContext {

    private int vertexCount_ = 0;
    private int edgeCount_ = 0;


    //vertex id -> variable name
    private Map<IBMGraphVertex,String> vertexVarNames_ = new HashMap<>();
    private Map<IBMGraphEdge,String> edgeVarNames_ = new HashMap<>();
    private List<GroovyExpression> variableDefs_ = new ArrayList<GroovyExpression>();


    private GroovyExpression graph_ = new IdentifierExpression("graph");
    private IBMGraphGraph graphInstance_ =  null;

    private boolean graphTraversalAdded_ = false;

    private List<IBMGraphElement> newElements_ = new ArrayList<>();

    public ActionTranslationContext(IBMGraphGraph graph) {
        graphInstance_ = graph;
    }

    public List<IBMGraphElement> getNewElements() {
        return newElements_;
    }

    public void addNewEdge(IBMGraphEdge edge) {
        if(newElements_.contains(edge)) {
            return;
        }
        GroovyExpression outVertexExpr = getVertexReferenceExpression(edge.getOutVertex().getV());
        GroovyExpression inVertexExpr = getVertexReferenceExpression(edge.getInVertex().getV());
        GroovyExpression expr = new FunctionCallExpression(outVertexExpr, "addEdge", new LiteralExpression(edge.getLabel()), inVertexExpr);

        String varName = "edge" + (edgeCount_++);
        variableDefs_.add(new VariableAssignmentExpression("Edge", varName, expr));
        edgeVarNames_.put(edge, varName);
        newElements_.add(edge);
    }

    public void addNewVertex(IBMGraphVertex vertex) {
        if(newElements_.contains(vertex)) {
            return;
        }
        GroovyExpression expr = new FunctionCallExpression(getGraphExpr(), "addVertex");
        String varName = "vertex" + (vertexCount_++);
        variableDefs_.add(new VariableAssignmentExpression("Vertex", varName, expr));
        vertexVarNames_.put(vertex, varName);
        newElements_.add(vertex);
    }

    public GroovyExpression getElementReferenceExpression(IBMGraphElement element) {
        if(element instanceof IBMGraphEdge) {
            return getEdgeReferenceExpression((IBMGraphEdge)element);
        }
        return getVertexReferenceExpression((IBMGraphVertex)element);

    }

    public GroovyExpression getVertexReferenceExpression(IBMGraphVertex vertex) {
        String varName = vertexVarNames_.get(vertex);
        if(varName == null) {
            String vertexId = vertex.getId().toString();
            GroovyExpression expr = new GetElementExpression(getGraphExpr(), ElementType.VERTEX, vertexId);
            varName = "vertex" + (vertexCount_++);

            variableDefs_.add(new VariableAssignmentExpression("Vertex", varName, expr));
            vertexVarNames_.put(vertex, varName);
        }
        return new IdentifierExpression(varName);
    }

    public GroovyExpression getEdgeReferenceExpression(IBMGraphEdge edge) {

        String varName = edgeVarNames_.get(edge);
        if(varName == null) {
            String edgeId = edge.getId().toString();
            GroovyExpression expr = new GetElementExpression(getGraphExpr(), ElementType.EDGE, edgeId);
            varName = "edge" + (edgeCount_++);

            variableDefs_.add(new VariableAssignmentExpression("Edge", varName, expr));
            edgeVarNames_.put(edge, varName);
        }
        return new IdentifierExpression(varName);
    }

    public GroovyExpression getGraphExpr() {
        return graph_;
    }

    public List<GroovyExpression> getVariableDefinitions() {
        return variableDefs_;
    }

    public GroovyExpression getGraphTraversal() {
        if(! graphTraversalAdded_) {
            TenantGraphStrategy strategy = GraphDatabaseConfiguration.INSTANCE.getTenantGraphStrategy();
            GroovyExpression stmt = new VariableAssignmentExpression("g", strategy.getGraphTraversalExpression(graphInstance_.getTenantId()));

            variableDefs_.add(stmt);
            graphTraversalAdded_ = true;
        }
        return new IdentifierExpression("g");
    }
}
