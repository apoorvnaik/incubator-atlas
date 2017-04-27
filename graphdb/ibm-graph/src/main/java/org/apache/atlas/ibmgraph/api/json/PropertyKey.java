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

import org.apache.atlas.ibmgraph.api.Cardinality;

/**
 *
 */
public class PropertyKey {

    private String name;

    private PropertyDataType dataType;

    private Cardinality cardinality;

    public PropertyKey() {
        super();
    }

    public PropertyKey(String name, PropertyDataType dataType,
            Cardinality cardinality) {
        super();
        this.name = name;
        this.dataType = dataType;
        this.cardinality = cardinality;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the datatype
     */
    public PropertyDataType getDatatype() {
        return dataType;
    }

    /**
     * @param datatype the datatype to set
     */
    public void setDatatype(PropertyDataType datatype) {
        this.dataType = datatype;
    }

    /**
     * @return the cardinality
     */
    public Cardinality getCardinality() {
        return cardinality;
    }

    /**
     * @param cardinality the cardinality to set
     */
    public void setCardinality(Cardinality cardinality) {
        this.cardinality = cardinality;
    }

     @Override
    public int hashCode() {
        int result = 17;
        result = 37*result + name.hashCode();
        return result;
    }

     @Override
    public boolean equals(Object other) {
        if(!(other instanceof PropertyKey)) {
            return false;
        }
        PropertyKey otherKey = (PropertyKey)other;
        return name.equals(otherKey.getName());
    }

    @Override
    public String toString() {
        return name;
    }


}
