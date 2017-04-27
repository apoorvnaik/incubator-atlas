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
package org.apache.atlas.ibmgraph.api.json.update;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * List of changes to a particular element in the format expected by
 * graphUpdater.groovy
 *
 */
public class ElementChanges {

    private final ElementType type;
    private String id;
    private boolean isDelete = false;
    private List<String> propertiesToRemove = null;

    //for multi-properties, the value must be a list with all
    //of the property values that are being added
    private Map<String, Object> propertiesToSet = null;
    private List<ElementIdListInfo> edgeIdListPropertiesToSet = null;
    private List<ElementIdListInfo> vertexIdListPropertiesToSet = null;
    private Map<String, String> vertexIdPropertiesToSet = null;
    private Map<String, String> edgeIdPropertiesToSet = null;

    public ElementChanges(ElementType type) {
        this.type = type;
    }

    public List<String> getPropertiesToRemove() {
        if (propertiesToRemove == null && !isDelete) {
            propertiesToRemove = new ArrayList<>();
        }
        return propertiesToRemove;
    }

    public Map<String, Object> getPropertiesToSet() {
        if (propertiesToSet == null) {
            propertiesToSet = new HashMap<>();
        }
        return propertiesToSet;
    }

    public List<ElementIdListInfo> getVertexIdListPropertiesToSet() {
        if (vertexIdListPropertiesToSet == null) {
            vertexIdListPropertiesToSet = new ArrayList<>();
        }
        return vertexIdListPropertiesToSet;
    }

    public List<ElementIdListInfo> getEdgeIdListPropertiesToSet() {
        if (edgeIdListPropertiesToSet == null) {
            edgeIdListPropertiesToSet = new ArrayList<>();
        }
        return edgeIdListPropertiesToSet;
    }

    public Map<String, String> getVertexIdPropertiesToSet() {
        if (vertexIdPropertiesToSet == null) {
            vertexIdPropertiesToSet = new HashMap<>();
        }
        return vertexIdPropertiesToSet;
    }

    public Map<String, String> getEdgeIdPropertiesToSet() {
        if (edgeIdPropertiesToSet == null) {
            edgeIdPropertiesToSet = new HashMap<>();
        }
        return edgeIdPropertiesToSet;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setIsDelete(boolean b) {
        isDelete = b;
    }

    public void addNewMultiPropertyValue(String key, Object value) {
        List existingValue = (List) getPropertiesToSet().get(key);
        if (existingValue == null) {
            existingValue = new ArrayList<>();
            propertiesToSet.put(key, existingValue);
        }
        existingValue.add(value);
    }

    public boolean isNoOp() {
        return !isDelete &&
                propertiesToRemove == null &&
                propertiesToSet == null &&
                edgeIdListPropertiesToSet == null &&
                vertexIdListPropertiesToSet == null &&
                vertexIdPropertiesToSet == null &&
                edgeIdPropertiesToSet == null;
    }

}