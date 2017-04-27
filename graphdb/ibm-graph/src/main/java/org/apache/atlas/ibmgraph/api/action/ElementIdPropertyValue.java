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
package org.apache.atlas.ibmgraph.api.action;

import org.apache.atlas.ibmgraph.IBMGraphElement;
import org.apache.atlas.ibmgraph.IBMGraphVertex;
import org.apache.atlas.ibmgraph.api.json.update.ElementChanges;
import org.apache.atlas.ibmgraph.util.GraphDBUtil;
import org.apache.atlas.repository.graphdb.AtlasElement;
/**
 * Property whose value is a id of the element specified.  This class has been carefully created to avoid physically
 * creating the element in the graph (if it does not exist).  For new elements,
 * the property value is set by retrieving the element id programatically within
 * the Gremlin Groovy script.
 *
 *
 */
public class ElementIdPropertyValue implements IPropertyValue {

    private AtlasElement element_;


    public ElementIdPropertyValue(AtlasElement element) {
        element_ = element;
    }

    @Override
    public <T> T getValue(Class<T> clazz) {
        if(AtlasElement.class.isAssignableFrom(clazz)) {
            return (T)element_;
        }
        if(! element_.exists()) {
            return null;
        }
        return (T)element_.getId().toString();
    }

    @Override
    public boolean isIndexed() {
        return false;
    }

    @Override
    public Class getOriginalType() {
        return String.class;
    }

    @Override
    public String toString() {
        return "Id of " + element_;
    }

    @Override
    public void addToElementChanges(String propertyName, ElementChanges changes) {

        IBMGraphElement<?> graphElement = GraphDBUtil.unwrapIfNeeded(element_);
        String id = graphElement.getIdOrLocalId();
        if (element_ instanceof IBMGraphVertex) {
            changes.getVertexIdPropertiesToSet().put(propertyName, id);
        } else {
            changes.getEdgeIdPropertiesToSet().put(propertyName, id);
        }
    }

}
