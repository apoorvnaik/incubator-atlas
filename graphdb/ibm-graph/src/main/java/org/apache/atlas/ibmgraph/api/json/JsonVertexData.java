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

package org.apache.atlas.ibmgraph.api.json;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Represents a vertex in a consolidate format that includes the vertex
 * and its outgoing edges.  This format is returned by the gremlin DSL queries
 * and the majority of the logic that retrieves vertices from IBM Graph.
 */
public class JsonVertexData {

    private JsonVertex vertex;

    private List<JsonEdge> outgoingEdges;

    public JsonVertexData() {
        vertex = new JsonVertex();
        outgoingEdges = new ArrayList<>();
    }

    public JsonVertexData(JsonVertex vertex) {
        this.vertex = vertex;
    }

    public JsonVertex getVertex() {
        return vertex;
    }

    public void setVertex(JsonVertex vertex) {
        this.vertex = vertex;
    }

    public Collection<JsonEdge> getOutgoingEdges() {
        return outgoingEdges;
    }

    public void setOutgoingEdges(List<JsonEdge> outgoingEdges) {
        this.outgoingEdges = outgoingEdges;
    }

}