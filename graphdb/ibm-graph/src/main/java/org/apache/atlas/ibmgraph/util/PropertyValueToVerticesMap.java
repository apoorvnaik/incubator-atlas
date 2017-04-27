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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.atlas.ibmgraph.IBMGraphElement;

/**
 * Map of property values to the vertices with that property value.  This
 * is used by the PropertyIndex.
 */
public class PropertyValueToVerticesMap {

    private Map<String,Set<IBMGraphElement>> valueToVertices_ = new HashMap<>();


    public void recordPropertySet(IBMGraphElement vertex, Object value) {
        String key = getMapKey(value);
        Set<IBMGraphElement> vertices = valueToVertices_.get(key);
        if(vertices == null) {
            vertices = new HashSet<>();
            valueToVertices_.put(key, vertices);
        }
        vertices.add(vertex);
    }

    public void recordPropertyRemove(IBMGraphElement vertex, Object value) {
        Collection<IBMGraphElement> vertices = valueToVertices_.get(getMapKey(value));
        if(vertices == null) {
            return;
        }
        vertices.remove(vertex);

    }

    public Set<IBMGraphElement> getElements(Object value) {
        Set<IBMGraphElement> vertices = valueToVertices_.get(getMapKey(value));
        if(vertices == null) {
            return Collections.emptySet();
        }
        return vertices;
    }

    private String getMapKey(Object value) {
        return String.valueOf(value);
    }
}