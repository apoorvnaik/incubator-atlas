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

/* 
 * IMPORTANT : This script will be minified before sending it to IBM Graph.  Specifically,
 * all newlines, comments, and extra whitespace will be removed.  Because of this,
 * there MUST be semicolons BETWEEN statements, even though that is not normally
 * required in Groovy.
 *
 * This script is used do perform all creation and updates of vertices
 * and edges in IBM Graph.  See UpdateScriptBinding for details
 * about the format of the expected bindings.  Generally, the script
 * requires 3 binding parameters : "newVertices", "newEdges", and "elementChanges".
 * The output of the script is a 2 element array that has the ids of the
 * vertices and edges that were created by the script.
 *
 * New vertices and edges must be added to newVertices/newEdges.  Part
 * of the information about each new vertex and edges is a localId that
 * it is associated with.  In the other parts of the binding, for example, 
 * in the id field of elementChanges, the localId should be used as
 * the id of the vertex or edge.  The script will replace that with the
 * id that was assigned by IBM Graph.
 */
 
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategy;
  
<IF_LOGGING>
def theLog = "";

def log = { msg ->
    theLog = String.valueOf("${theLog}${msg}\n");
};
</IF_LOGGING>

try {
   
    def g = <TRAVERSAL_SOURCE_EXPRESSION>;   
    def vertexIdMap = [:];
    def edgeIdMap = [:];
    
    def createdVertices = [];
    def createdEdges = [];
    def listDelimiter = String.valueOf((char)3);
    def listPrefix = String.valueOf("${(char)2}list:");


    def getEdgeId = { id -> 
        def value = edgeIdMap.get(id); 
        if(value != null) {
            value
        } else {
            id
        } 
    };
    
    def getVertexId = {  id -> 
        def value = vertexIdMap.get(id);
        if(value != null) {
            value
        } else {
            id
        } 
    };
    
    //generates a String which is a delimited list containing
    //the specified ids in the format required by our code.
    //Local ids are replaced with the ids of elements that
    //have been created. 
    def buildIdList = { boolean isVertexList, List ids ->
        Iterator it = ids.iterator();
        def value=listPrefix;
        while(it.hasNext()) {
            def nextId = it.next();
            if(isVertexList) {
                nextId = getVertexId(nextId);
            }
            else {
                nextId = getEdgeId(nextId);
            };
            value=String.valueOf("${value}${nextId}");
            if(it.hasNext()) {
                value=String.valueOf("${value}${listDelimiter}")
            }
        };
        value;
    };   
    
    //create new vertices, add generated id to vertexIdMap
    newVertices.each {
        String localId = (String)it;
        //create the vertex using the graph traversal to ensure that it
        //gets added to the partition.
        Vertex v = g.addV().toList().get(0);
        createdVertices.push(v.id());
        vertexIdMap.put(localId, v.id().toString());
        <IF_LOGGING>log("Created vertex: ${v.toString()} with id ${v.id()}");</IF_LOGGING>
    };
    
    //create new edges, add generated id to edgeIdMap
    newEdges.each {
        def newEdge = (Map)it;
        String localId = newEdge.localId;
        String label = newEdge.label;
        String inVId = getVertexId(newEdge.inV);
        String outVId = getVertexId(newEdge.outV); 
        def inV = graph.vertices(inVId).getAt(0);
        def outV = graph.vertices(outVId).getAt(0);
        Edge e = outV.addEdge(label, inV);
        createdEdges.push(e.id());
        edgeIdMap.put(localId, e.id().toString())
    };
    
    
    for(Map elementChange in elementChanges) {
        def type=elementChange.type;
        Element elem;
        def isVertex = type == "vertex";
        
        <IF_LOGGING>log("Processing changes for ${type} ${elementChange.id}");</IF_LOGGING>
        
        if(isVertex) {
            def id = getVertexId(elementChange.id);
            elem = graph.vertices(id).getAt(0)
        }
        else {
            def id = getEdgeId(elementChange.id);
            elem = graph.edges(id).getAt(0)
        };
        
        <IF_LOGGING>log("Mapped element to ${elem}");</IF_LOGGING>
        
        if(elementChange.isDelete) {
            //elem will be null if element was previously deleted
            if(elem != null) {
                elem.remove()
            }
        };
        
        //Clear required properties.  Clears must come before sets, in case they
        //are clearing and then adding new values.
        elementChange.propertiesToRemove.each { String name ->
            
           elem.properties(name).each { 
                it.remove()
            }
            
        };  
        
        //set required properties
        for(prop in (Map)elementChange.propertiesToSet) {
            String name = prop.getKey();
            def value = prop.getValue();
            if(value instanceof Collection) {
                 //handle multi-properties
                 value.each { item ->
                    <IF_LOGGING>log("Setting property ${name} to ${item}");</IF_LOGGING>
                    elem.property(name, item)
                }
            }
            else {
                <IF_LOGGING>log("Setting property ${name} to ${value != null ? String.valueOf(value.getClass()) : "null"} with value ${String.valueOf(value)}");</IF_LOGGING>
                elem.property(name, value)
            };
            
        };           
         
        //set properties whose value is a delimited list of
        //vertex ids
        elementChange.vertexIdListPropertiesToSet.each { Map it ->
            def value=buildIdList(true, (List)it.ids);
            elem.property(it.name.toString(), value)
        };
        
        //set properties whose value is a delimited list of
        //edge ids
        elementChange.edgeIdListPropertiesToSet.each { Map it -> 
            def value=buildIdList(false, (List)it.ids);
            elem.property(it.name.toString(), value)
        }; 
           
        //set properties whose value is the id of a vertex
        elementChange.vertexIdPropertiesToSet.each { Map.Entry x ->
            elem.property(x.getKey().toString(), getVertexId(x.getValue()))
        };
        
        //set properties whose value is the id of an edge
        elementChange.edgeIdPropertiesToSet.each { Map.Entry x ->
            elem.property(x.getKey().toString(), getEdgeId(x.getValue()))
        }    
    };
    //return the ids of the created vertices and edges to the caller of the script
    [true<IF_LOGGING>, theLog</IF_LOGGING>, createdVertices, createdEdges]
    
}
catch(t) {
    [false<IF_LOGGING>, theLog</IF_LOGGING>, String.valueOf(t)]
}