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

import org.apache.atlas.ibmgraph.api.json.update.ElementChanges;

/**
 * Represents the value of an Element property.
 */
public interface IPropertyValue {

    /**
     * Whether or not this value should be added to the property value index
     */
    public boolean isIndexed();

    /**
     * Gets the value of the property, attempting to convert it to an instance of
     * the given class.
     */
    public <T> T getValue(Class<T> clazz);

    /**
     * Gets the type of the property value.
     */
    Class getOriginalType();

    /**
     * Updates changes to set the property with the given name to this value.
     *
     * @param propertyName
     * @param changes
     */
    void addToElementChanges(String propertyName, ElementChanges changes);

}
