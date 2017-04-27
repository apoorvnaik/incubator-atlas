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

package org.apache.atlas.ibmgraph.api.json;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Maps.EntryTransformer;

public class JsonEdge extends JsonGraphElement {


    private String inV;
    private String outV;
    private Map<String,Object> properties = new HashMap<>();


    /**
     * @return the outV
     */
    public String getOutV() {
        return outV;
    }

    /**
     * @return the inV
     */
    public String getInV() {
        return inV;
    }

    @Override
    public String toString() {
        return "JsonEdge [inV=" + inV + ", outV=" + outV + ", properties=" + properties
                + ", id=" + getId() + ", label=" + getLabel() + "]";
    }

    @Override
    public Map<String, Set<PropertyValue>> getProperties() {
        return Maps.transformEntries(properties, new EntryTransformer<String,Object,Set<PropertyValue>>() {
            @Override
            public Set<PropertyValue> transformEntry(String key, Object value) {
                PropertyValue propertyValue = new PropertyValue(null, value);
                return Collections.singleton(propertyValue);
            }
        });
    }

}