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

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.atlas.ibmgraph.IBMGraphElement;
import org.apache.atlas.ibmgraph.api.action.IPropertyValue;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

/**
 * A map of the current property values for an element, taking account changes
 * that have been applied within a transaction.  Property changes are applied
 * to an internal map.  When property values are requested, we first check
 * the map to see if the property has been updated.  If it has been updated,
 * we return the updated value.  Otherwise, we retrieve the property value
 * from the underlying element.
 *
 */
public class ElementBackedPropertyMap extends AbstractMap<String, Set<IPropertyValue>> {

    //the underlying graph element we get the properties from
    private IBMGraphElement<?> element_;

    //new values for properties that have been changed
    private Map<String,Set<IPropertyValue>> updatedPropertyValues_ = new HashMap<>();

    private Set<String> updatedMultiPropertyNames_ = new HashSet<String>();

    //properties that have been removed by the user.  Note that properties can be removed
    //and added back within a transaction.
    private Set<String> clearedProperties_ = new HashSet<String>();

    //we need to separately track removed property values.  This is because, for multi-properties,
    //null indicates that the property values have not been initialized.  This should
    //not be confused with whether or not the property was removed.

    public ElementBackedPropertyMap(IBMGraphElement<?> element) {
        element_ = element;
    }

    @Override
    public Set<IPropertyValue> put(String key, Set<IPropertyValue> value) {
        Set<IPropertyValue> oldValue = get(key);
        updatedPropertyValues_.put(key, value);
        return oldValue;
    }

    @Override
    public Set<IPropertyValue> remove(Object key) {
        Set<IPropertyValue> oldValue = get(key);
        clearedProperties_.add(String.valueOf(key));
        updatedPropertyValues_.remove(key);
        return oldValue;
    }

    @Override
    public void clear() {
        updatedPropertyValues_.clear();
        updatedMultiPropertyNames_.clear();
        for(String key : element_.getCommittedPropertyNames()) {
            clearedProperties_.add(key);
        }
    }

    @Override
    public Set<String> keySet() {

        //the keyset is computed by combining the properties in the underlying element (filtering out
        //properties that have been removed) with the keys for property values that have been added or updated.
        //This is created in such a way that it always reflects the keys present in the map, even if changes
        //are made after the keyset is returned.

        Set<String> nonRemoved = Sets.<String>filter(element_.getCommittedPropertyNames(), new Predicate<String>() {

            @Override
            public boolean apply(String input) {
                return ! clearedProperties_.contains(input);
            }

        });

        return Sets.union(nonRemoved, updatedPropertyValues_.keySet());
    }

    @Override
    public boolean containsKey(Object key) {
        if (updatedPropertyValues_.containsKey(key)) {
            return true;
        }
        if(clearedProperties_.contains(key)) {
            return false;
        }
        return element_.getCommittedPropertyNames().contains(key);
    }

    @Override
    public Set<IPropertyValue> get(Object key) {

        //cleared properties may have been set to a new value, so checked the updated properties
        //before the cleared ones.
        Set<IPropertyValue> result = updatedPropertyValues_.get(key);
        if( result != null) {
            return augmentPropertyValue(String.valueOf(key), result);
        }

        if(clearedProperties_.contains(key)) {
            return null;
        }
        return element_.getCommittedPropertyValues(key);
    }

    @Override
    public Set<Entry<String, Set<IPropertyValue>>> entrySet() {


        //creates a dynamic entrySet that always reflects the contents of the map, even if changes
        //are made after the entrySet is returned.

        Set<Entry<String, Set<IPropertyValue>>> result = new AbstractSet<Entry<String, Set<IPropertyValue>>>() {

            @Override
            public Iterator<Map.Entry<String, Set<IPropertyValue>>> iterator() {
                return Iterators.transform(keySet().iterator(), new Function<String,Map.Entry<String, Set<IPropertyValue>>>() {

                    @Override
                    public java.util.Map.Entry<String, Set<IPropertyValue>> apply(String input) {
                        return new StorageBackedPropertyMapEntry(input);
                    }

                });
            }

            @Override
            public int size() {
                return keySet().size();
            }
        };

        return result;
    }

    /**
     * Applies the property changes we've seen to the given map.  This is used during
     * commit to update the property value map in the underlying element.
     */
    public void applyChanges(Map<String, Set<IPropertyValue>> toUpdate) {
        for(String removed : clearedProperties_) {
            toUpdate.remove(removed);
        }
        for(Map.Entry<String, Set<IPropertyValue>> propertyValue : updatedPropertyValues_.entrySet()) {
            String name = propertyValue.getKey();
            Set<IPropertyValue> newValue = augmentPropertyValue(name, propertyValue.getValue());
            toUpdate.put(name,  newValue);

        }
    }

    private Set<IPropertyValue> augmentPropertyValue(String name, Set<IPropertyValue> updatedValue) {

        Set<IPropertyValue> result = updatedValue;
        if(! hasClear(name)) {
            if(updatedMultiPropertyNames_.contains(name)) {
                result = new HashSet<IPropertyValue>();
                result.addAll(updatedValue);
                result.addAll(getCommittedPropertyValues(name));
            }
        }
        return result;
    }

    /**
     * Rolls back any changes that were made, so that the property values
     * are restored to the committed values in the underlying element.
     */
    public void clearChanges() {
        updatedPropertyValues_.clear();
        clearedProperties_.clear();
        updatedMultiPropertyNames_.clear();
    }

    /**
     * Map.Entry implementation for ElementBackedPropertyMap
     *
     */
    private final class StorageBackedPropertyMapEntry implements Map.Entry<String, Set<IPropertyValue>> {

        private final String propertyKey_;

        private Set<IPropertyValue> value_ = null;

        private StorageBackedPropertyMapEntry(String propertyValue) {
            this.propertyKey_ = propertyValue;
        }

        @Override
        public String getKey() {
            return propertyKey_;
        }

        @Override
        public Set<IPropertyValue> getValue() {
            return getValue_();
        }

        @Override
        public Set<IPropertyValue> setValue(Set<IPropertyValue> value) {
            throw new UnsupportedOperationException();
        }

        private Set<IPropertyValue> getValue_() {
            //defer computation of property value
            if(value_ == null) {
                value_ = get(propertyKey_);
            }
            return value_;
        }
    }

    public Set<String> getClearedProperties() {
        return Collections.unmodifiableSet(clearedProperties_);
    }

    public Map<String, Set<IPropertyValue>> getUpdatedProperties() {
        return Collections.unmodifiableMap(updatedPropertyValues_);
    }

    private boolean hasClear(String propertyName) {
        return clearedProperties_.contains(propertyName);
    }

    private Set<IPropertyValue> getCommittedPropertyValues(String propertyName) {
        Set<IPropertyValue> committedPropertyValues = element_.getCommittedPropertyValues(propertyName);
        if(committedPropertyValues == null) {
            return Collections.emptySet();
        }
        return committedPropertyValues;
    }

    //for multi-properties only
    public void addAdditionalPropertyValue(String name, IPropertyValue newValue) {

        //if there is a clear, all property values are new and need to be added to
        //updatedPropertyValues_
        if(! hasClear(name) && getCommittedPropertyValues(name).contains(newValue)) {

            return;
        }
        updatedMultiPropertyNames_.add(name);
        Set<IPropertyValue> updatedValues = updatedPropertyValues_.get(name);
        if(updatedValues == null) {
            updatedValues = new HashSet<IPropertyValue>();
            updatedPropertyValues_.put(name, updatedValues);
        }
        updatedValues.add(newValue);
    }

}
