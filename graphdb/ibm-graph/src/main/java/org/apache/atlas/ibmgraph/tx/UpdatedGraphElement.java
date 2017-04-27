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

import org.apache.atlas.ibmgraph.api.action.IPropertyValue;
import org.apache.atlas.ibmgraph.api.json.update.ElementChanges;

/**
 * Represents the updated state of an element within a transaction
 *
 */
public interface UpdatedGraphElement extends ReadableUpdatedGraphElement{

    /**
     * Applies the accumulated changes to the underlying element so that they
     * are seen by other users.  This is called as part of the process of
     * committing the transaction.
     */
    void pushChangesIntoElement();

    /**
     * Sets the value of a given property
     *
     * @param name the property name
     * @param newValue the new value of the property.
     * @return
     */
    IPropertyValue replacePropertyValueInMap(String name, IPropertyValue newValue);

    /**
     * Adds an additional value to a multi-property.
     *
     * @param name the property name
     * @param newValue the new property value to add.
     */
    void addAdditionalPropertyValueInMap(String name, IPropertyValue newValue);

    /**
     * Marks this element as being deleted within the current transaction.
     */
    void delete();

    ElementChanges convertToElementChanges();
}
