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

import org.apache.atlas.ibmgraph.IBMGraphEdge;
import org.apache.atlas.ibmgraph.IBMGraphElement;
import org.apache.atlas.ibmgraph.IBMGraphGraph;
import org.apache.atlas.ibmgraph.IBMGraphGraphQuery;
import org.apache.atlas.ibmgraph.IBMGraphVertex;
import org.apache.atlas.ibmgraph.api.action.IPropertyValue;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.tinkerpop.gremlin.structure.T;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;


/**
 * This class keeps track of the vertices associated with each value of a given property for
 * vertices that have been modified since the pending changes were last pushed into the graph.
 * There is one of these per IBMGraphDatabaseGraph instance.
 *
 * We maintain a map of property values to vertices that have that value for every property.  The map only contains
 * vertices whose property values have been changed since the last push into the graph.  For those vertices, all of
 * the property values for the vertex are in the index.  This map gets updated every time a property
 * value is changed/added/removed or a vertex is deleted.
 *
 * When an element is added to the index, all of its properties are indexed, not just the changed ones.
 *
 * @see IBMGraphElement#removeProperty(String)
 * @see IBMGraphElement#updatePropertyValueInMap(String, T)
 * @see IBMGraphVertex#updatePropertyValueInMap(String, T)
 * @see IBMGraphGraph#removeVertex(AtlasVertex)
 * @see IBMGraphDatabaseGraphQuery.vertices()
 *
 *
 */
public class PropertyIndex {

    private Map<String, PropertyValueToVerticesMap> indices_ = new HashMap<>();
    private Set<IBMGraphElement> trackedElements_ = new HashSet<>();

    public PropertyIndex() {

    }

    //only supports equality-based lookups


    /**
     * Finds elements with property changes that have not been pushed into IBM Graph
     * which have the specified property value (taking account the changes
     * that have been made locally).  For multi-properties, this will find vertices
     * whose property value contains the given value.
     *
     */
    public Set<IBMGraphElement> lookupMatchingElements(String property, Object value) {
        PropertyValueToVerticesMap map = indices_.get(property);
        if(map == null) {
            return Collections.emptySet();
        }
        return map.getElements(value);
    }

    /**
     * Finds vertices with property changes that have not been pushed into IBM Graph
     * which have the specified property value (taking account the changes
     * that have been made locally).  For multi-properties, this will find vertices
     * whose property value contains the given value.
     *
     */
    public Set<IBMGraphVertex> lookupMatchingVertices(String property, Object value) {
        Set elements = lookupMatchingElements(property, value);
        //we use AtlasElement here in order to comply with the contract
        //for apply, ie that A.equals(B) ==> apply(A) == apply(B).  This gets complicated
        //by the java proxying mechanism, which makes it possible for A to equal B if A is a java proxy
        //and B is an IBMGraphElement.  This resulted in test failures.
        return Sets.filter(elements, new Predicate<AtlasElement>() {

            @Override
            public boolean apply(AtlasElement input) {
                return  (input instanceof AtlasVertex);
            }
        });
    }

    /**
     * Finds edges with property changes that have not been pushed into IBM Graph
     * which have the specified property value (taking account the changes
     * that have been made locally).
     */
    public Set<IBMGraphEdge> lookupMatchingEdges(String property, Object value) {
        Set elements = lookupMatchingElements(property, value);

        //we use AtlasElement here in order to comply with the contract
        //for apply, ie that A.equals(B) ==> apply(A) == apply(B).  This gets complicated
        //by the proxying mechanism, which makes it possible for A to equal B if a a proxy
        //and B is an IBMGraphElement.  This resulted in test failures.
        return Sets.filter(elements, new Predicate<AtlasElement>() {

            @Override
            public boolean apply(AtlasElement input) {
                return  (input instanceof AtlasEdge);
            }
        });
    }

    /**
     * Updates the index to reflect that the specified property now
     * has the given value.  This should only be used for multi-properties.
     *
     * @param vertex the vertex that was changed
     * @param property the name of the property
     * @param newValue the new value that was added to the property
     */
    public void onPropertyAdd(IBMGraphVertex vertex, String property, Object newValue) {
        addOtherPropertiesIfNeeded(vertex, property);
        onPropertyAdd_(vertex, property, newValue);
    }

    /**
     * Updates the index to reflect that value of specified property has changed.  The
     * old value is dissociated from the element in the index, and an assocation is
     * added between the new value and the element.
     *
     * This should NOT be used for multi-properties.
     *
     * @param element the element that was modified
     * @param property the property name
     * @param newValue the new value of the property
     * @param oldValue the previous value of the property
     */
    public void onPropertySet(IBMGraphElement element, String property, Object oldValue, Object newValue) {
        addOtherPropertiesIfNeeded(element, property);
        PropertyValueToVerticesMap map = onPropertyAdd_(element, property, newValue);
        map.recordPropertyRemove(element, oldValue);
    }


    /**
     * Updates the index to reflect that value of specified property has been removed from
     * the given element.  All of the specified old values are dissociated from the element
     * in the index.
     *
     * @param element the element that was modified
     * @param property the property name
     * @param oldValues the previous values of the property
     */
    public void onPropertyRemove(IBMGraphElement element, String property, Collection<Object> oldValues) {

        addOtherPropertiesIfNeeded(element, property);
        onPropertyRemove_(element, property, oldValues);
    }

    /**
     * Updates the index to reflect that the given vertex has been deleted.
     *
     * @param the vertex that was deleted
     */
    public void onVertexDeleted(IBMGraphVertex vertex) {
        for(String key : vertex.getPropertyKeys()) {
            onPropertyRemove_(vertex, key, vertex.getPropertyValues(key, Object.class));
        }
    }

    /**
     * Clears the index
     *
     */
    public void clear() {
        indices_.clear();
        trackedElements_.clear();
    }

    private PropertyValueToVerticesMap onPropertyAdd_(IBMGraphElement vertex, String property, Object newValue) {
        PropertyValueToVerticesMap map = getMap(property);
        map.recordPropertySet(vertex, newValue);
        return map;
    }

    private void onPropertyRemove_(IBMGraphElement vertex, String property, Collection<Object> oldValues) {
        PropertyValueToVerticesMap map = getMap(property);
        if(oldValues != null) {
            for(Object oldValue : oldValues) {
                map.recordPropertyRemove(vertex, oldValue);
            }
        }
    }

    private PropertyValueToVerticesMap getMap(String property) {
        PropertyValueToVerticesMap map = indices_.get(property);
        if(map == null) {
            map = new PropertyValueToVerticesMap();
            indices_.put(property, map);
        }
        return map;
    }

    /**
     * In order for query results in {@link IBMGraphGraphQuery} to be correct, all of the properties
     * in modified vertices need to be in the index.  This logic adds the properties that are *not*
     * being modified into the index.  This is only done once per Vertex.  We keep track of which vertices
     * we have already processed to avoid processing vertices multiple times.
     *
     * @param element
     * @param propertyToExclude
     */
    private void addOtherPropertiesIfNeeded(IBMGraphElement element, String propertyToExclude) {

        if(trackedElements_.contains(element)) {
            return;
        }
        trackedElements_.add(element);

        Map<String, Set<IPropertyValue>> readOnlyPropertyValues = element.getReadOnlyPropertyValues();
        for(Map.Entry<String, Set<IPropertyValue>> propertiesEntry : readOnlyPropertyValues.entrySet()) {
            String propertyName = propertiesEntry.getKey();
            Collection<IPropertyValue> values = propertiesEntry.getValue();
            if(propertyName.equals(propertyToExclude)) {
                continue;
            }
            for(IPropertyValue value : values) {
                if(! value.isIndexed()) {
                    continue;
                }
                Object converted = element.transformPropertyValue(value, Object.class);
                onPropertyAdd_(element, propertyName, converted);
            }
        }
    }
}