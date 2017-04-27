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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.apache.atlas.ibmgraph.IBMGraphElement;
import org.apache.atlas.ibmgraph.api.json.update.ElementChanges;
import org.apache.atlas.ibmgraph.api.json.update.ElementIdListInfo;
import org.apache.atlas.ibmgraph.util.GraphDBUtil;
import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasVertex;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * Property whose value is a String list consisting of the ids of the elements
 * specified.  This class has been carefully created to avoid physically
 * creating the element in the graph (if it does not exist).  For new elements,
 * the property value is set by retrieving the element id programatically within
 * the Gremlin Groovy script.
 *
 *
 */
public class ElementIdListPropertyValue implements IPropertyValue {

    private List<AtlasElement> elements_;


    public ElementIdListPropertyValue(List<AtlasElement> elements) {
        elements_ = elements;
    }

    @Override
    public <T> T getValue(Class<T> clazz) {

        if(AtlasElement.class.isAssignableFrom(clazz)) {
            return (T)elements_;
        }

        List<String> idList = Lists.transform(elements_, new Function<AtlasElement,String>() {

            @Override
            public String apply(AtlasElement input) {
                Object id  = input.getId();
                if(id == null) {
                    return null;
                }
                return id.toString();
            }
        });

        //filter out null values
        idList.removeIf(new Predicate<String>() {

            @Override
            public boolean test(String t) {
                return t == null;
            }
        });

        return (T)idList;

    }

    @Override
    public boolean isIndexed() {
        return false;
    }

    @Override
    public Class getOriginalType() {
        return List.class;
    }

    @Override
    public String toString() {
        return "Ids of " + elements_;
    }

    @Override
    public void addToElementChanges(String propertyName, ElementChanges changes) {

        if (elements_.size() == 0) {
            return;
        }

        List<String> ids = new ArrayList<>(elements_.size());
        boolean isVertexList = elements_.get(0) instanceof AtlasVertex;
        for(AtlasElement element : elements_) {
            IBMGraphElement<?> graphElement = GraphDBUtil.unwrapIfNeeded(element);
            String id = graphElement.getIdOrLocalId();
            ids.add(id);
        }

        ElementIdListInfo info = new ElementIdListInfo(propertyName, ids);
        if (isVertexList) {
            changes.getVertexIdListPropertiesToSet().add(info);
        } else {
            changes.getEdgeIdListPropertiesToSet().add(info);
        }
    }

}
