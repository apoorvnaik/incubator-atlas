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

package org.apache.atlas.ibmgraph.tx;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.atlas.ibmgraph.IBMGraphElement;
import org.apache.atlas.ibmgraph.api.action.IPropertyValue;
import org.apache.atlas.ibmgraph.api.json.update.ElementChanges;
import org.apache.atlas.ibmgraph.util.GraphDBUtil;

/**
 * Represents the updated state of an AtlasAtlasElement, with
 * the property changes through the DeleteContext applied.
 *
 */
public abstract class DefaultUpdatedGraphElement<T extends IBMGraphElement> implements UpdatedGraphElement {

    protected T wrappedElement_;

    private ElementBackedPropertyMap propertyValues_ = null;

    //Whether this element has been deleted during the transaction
    private boolean isDeleted_ = false;

    protected IBMGraphTransaction tx_;

    protected abstract ElementChanges createElementChangesInstance();

    public DefaultUpdatedGraphElement(IBMGraphTransaction tx, T element) {

        wrappedElement_ = element;
        propertyValues_ = new ElementBackedPropertyMap(element);
        isDeleted_ = element.isDeletedInGraph();
        tx_ = tx;
    }

    @Override
    public boolean isDeleted() {
        return isDeleted_;
    }


    @Override
    public ElementBackedPropertyMap getPropertyValues() {
        return propertyValues_;
    }

    @Override
    public Map<String,Set<IPropertyValue>> getReadOnlyPropertyValues() {

        return Collections.unmodifiableMap(propertyValues_);
    }

    @Override
    public IPropertyValue replacePropertyValueInMap(String name, IPropertyValue newValue) {

        IPropertyValue oldValue = GraphDBUtil.asSingleValue(propertyValues_.get(name));
        propertyValues_.put(name, Collections.singleton(newValue));
        return oldValue;

    }

    //only called for multi-properties
    @Override
    public void addAdditionalPropertyValueInMap(String name, IPropertyValue newValue) {

        propertyValues_.addAdditionalPropertyValue(name, newValue);
    }

    @Override
    public void pushChangesIntoElement() {
        //The property updates are done in a somewhat convoluted way to avoid
        //directly exposing the underlying property value map to callers
        //of the vertex
        wrappedElement_.applyChangesFromTransaction(this);

    }

    @Override
    public void delete() {
        isDeleted_ = true;
    }

    @Override
    public ElementChanges convertToElementChanges() {
        ElementChanges changes = createElementChangesInstance();
        changes.setId(wrappedElement_.getIdOrLocalId());
        if (isDeleted_) {
            changes.setIsDelete(true);
        }
        else {
            for (String clearedProperty : propertyValues_.getClearedProperties()) {
                changes.getPropertiesToRemove().add(clearedProperty);
            }
            for (Map.Entry<String, Set<IPropertyValue>> entry : propertyValues_.getUpdatedProperties().entrySet()) {
                String name = entry.getKey();
                Set<IPropertyValue> values = entry.getValue();

                if (values.size() > 1) {
                    for (IPropertyValue value : values) {
                        //multi-properties are never used for element id lists or element ids
                        changes.addNewMultiPropertyValue(name, value.getValue(value.getOriginalType()));
                    }
                } else if (values.size() == 1) {
                    IPropertyValue value = values.iterator().next();
                    value.addToElementChanges(name, changes);
                }
            }
        }
        return changes;
    }

}