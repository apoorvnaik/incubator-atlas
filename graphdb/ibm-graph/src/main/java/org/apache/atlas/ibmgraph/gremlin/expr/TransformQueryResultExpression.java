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

package org.apache.atlas.ibmgraph.gremlin.expr;

import org.apache.atlas.groovy.*;
import org.apache.atlas.groovy.ClosureExpression.VariableDeclaration;
import org.apache.atlas.ibmgraph.gremlin.stmt.IfStatement;

import java.util.Collections;
import java.util.List;

/**
 * This is an expression that transforms all of the vertices in a query result
 * into JsonVertexData objects that contain the vertex and all of its
 * outgoing edges.  The logic is more complicated when result can be a
 * Path object, so a parameter was added to indicate whether that logic
 * is really needed.  In addition, certain queries, eg select
 * expressions that don't include a path statement, cannot return
 * vertices at all.  The logic here is optimized so that we only
 * include logic to handle the cases that are relevant to the gremlin
 * query whose result we are transforming.
 */
public class TransformQueryResultExpression extends AbstractFunctionExpression {

    private static final String XFORM_FUNCTION_NAME = "xform";
    private boolean isSelectExpr_;
    private boolean isPathExpr_;

    public TransformQueryResultExpression(GroovyExpression listExpr, boolean isSelect, boolean isPath) {
        super(listExpr);
        isSelectExpr_ = isSelect;
        isPathExpr_ = isPath;
    }

    /**
     * Generates a function that recursively replaces Vertices in maps,
     * paths, and lists with the VertexData data structure that contains both
     * the vertex and its list of outgoing edges.  We only include the logic
     * for handling paths if it is a path expression.
     * @return
     */
    private GroovyExpression generateTransformFunctionDefinition() {

        GroovyExpression functionArg = new IdentifierExpression("x");

        //if it's a vertex, transform the vertex
        GroovyExpression isVertexExpr = new InstanceOfExpression(functionArg, "Vertex");
        GroovyExpression xformVertexExpr = buildTransformVertexExpression(functionArg);

        IfStatement stmt = new IfStatement(isVertexExpr, xformVertexExpr);

        //if it's a list, recursively transform the list
        GroovyExpression isListExpr = new InstanceOfExpression(functionArg, "List");
        GroovyExpression xformListExpr = buildTransformListExpression(functionArg);
        stmt.addElseIf(isListExpr, xformListExpr);


        GroovyExpression isMapExpr = new InstanceOfExpression(functionArg, "Map");
        GroovyExpression xformMapExpression = buildTransformMapExpression(functionArg);
        stmt.addElseIf(isMapExpr, xformMapExpression);

        if(isPathExpr_) {
            //if it's a path, recursively transform the path
            GroovyExpression isPathExpr = new InstanceOfExpression(functionArg, "Path");
            GroovyExpression xformPathExpr = buildTransformPathExpression(functionArg);
            stmt.addElseIf(isPathExpr, xformPathExpr);
        }

        //if it's not a vertex, path, or list, return the original object
        stmt.setElseStatement(functionArg);

        ClosureExpression closure = new ClosureExpression(stmt, Collections.singletonList(new VariableDeclaration("Object", "x")));

        VariableAssignmentExpression expr = new VariableAssignmentExpression(XFORM_FUNCTION_NAME, closure);
        expr.setOmitDeclaration(true);
        return expr;

    }
    /**
     * Creates the expression: ((Path)x).objects()
     */
    private FunctionCallExpression buildTransformPathExpression(GroovyExpression functionArg) {
        return new FunctionCallExpression(new CastExpression(functionArg,"Path"),"objects");
    }
    /**
     * Builds the expression: [type:'vertexdata', vertex:((Vertex)x), outgoingEdges: ((Vertex)x).edges(Direction.OUT).toList()]
     * @param functionArg
     * @return
     */
    private GetVertexDataExpression buildTransformVertexExpression(GroovyExpression functionArg) {
        return new GetVertexDataExpression(new CastExpression(functionArg,"Vertex"), false);
    }

    private GroovyExpression buildTransformListExpression(GroovyExpression functionArg) {
        GroovyExpression xformListExpr = new CollectExpression(new CastExpression(functionArg,"List"), new FunctionCallExpression(XFORM_FUNCTION_NAME, new IdentifierExpression("it")));
        return xformListExpr;
    }
    /**
     * Generates the expression: ((Map)x).collectEntries({k, v->[k, xform(v)]})
     *
     * @param functionArg
     * @return
     */
    private GroovyExpression buildTransformMapExpression(GroovyExpression functionArg) {
        ClosureExpression xformMapClosure = new ClosureExpression("k","v");
        IdentifierExpression keyExpr = new IdentifierExpression("k");
        IdentifierExpression valueExpr = new IdentifierExpression("v");
        GroovyExpression transformedMapEntry = new ListExpression(
                keyExpr,
                new FunctionCallExpression(XFORM_FUNCTION_NAME, valueExpr));
        xformMapClosure.addStatement(transformedMapEntry);

        GroovyExpression xformMapExpression = new FunctionCallExpression(new CastExpression(functionArg,"Map"),"collectEntries",xformMapClosure);
        return xformMapExpression;
    }

    @Override
    public void generateGroovy(GroovyGenerationContext context) {

        StatementListExpression result = new StatementListExpression();
        //We need to explicitly declare the variable before we assign its value
        //since the closure body makes recursive calls (which require the variable
        //to be declared).
        result.addStatement(new VariableAssignmentExpression("Closure", XFORM_FUNCTION_NAME, null));
        result.addStatement(generateTransformFunctionDefinition());
        result.addStatement(new FunctionCallExpression(XFORM_FUNCTION_NAME, getCaller()));
        result.generateGroovy(context);
    }

    @Override
    public List<GroovyExpression> getChildren() {
        return Collections.singletonList(getCaller());
    }

    @Override
    public GroovyExpression copy(List<GroovyExpression> newChildren) {
        assert newChildren.size() == 1;
        return new TransformQueryResultExpression(newChildren.get(0), isSelectExpr_, isPathExpr_);
    }
}
