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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import org.apache.atlas.ibmgraph.api.action.IPropertyValue;
import org.apache.atlas.ibmgraph.api.json.update.ElementChanges;
import org.apache.atlas.repository.graphdb.AtlasElement;

import com.google.gson.JsonPrimitive;


/**
 *
 */
public class PropertyValue implements IPropertyValue {

    private String id;
    private JsonPrimitive value;
    private transient Class originalType_;

    public PropertyValue() {
        super();
    }

    public PropertyValue(String id, Object propertyValue) {
        super();
        this.id = id;
        value = new JsonPrimitive(String.valueOf(propertyValue));
        this.value = createPrimitive(propertyValue);
        originalType_ = propertyValue.getClass();
    }

    @Override
    public Class getOriginalType() {
        if(originalType_ != null) {
            return originalType_;
        }
        else {
            //value came in from json,
            return Object.class;
        }
    }


    private JsonPrimitive createPrimitive(Object value) {

        if(value instanceof Character) {
            return new JsonPrimitive((Character)value);
        }
        else if(value instanceof Number) {
            return new JsonPrimitive((Number)value);
        }
        else if(value instanceof String) {
            return new JsonPrimitive((String)value);
        }
        else if(value instanceof Boolean) {
            return new JsonPrimitive((Boolean)value);
        }
        else {
            return new JsonPrimitive(String.valueOf(value));
        }
    }
    /**
     * @return the id
     */
    public String getId() {
        return id;
    }
    /**
     * @param id the id to set
     */
    public void setId(String id) {
        this.id = id;
    }
    /**
     * @return the value
     */
    public Object getValue() {
        if(value == null) {
            return null;
        }
        return getValue(getOriginalType());
    }

    public JsonPrimitive getAsJsonPrimitive() {
        return value;
    }

    /**
     * @param value the value to set
     */
    public void setValue(Object value) {
        this.value = createPrimitive(value);
    }

    @Override
    public int hashCode() {
        if(value == null) {
            return 0;
        }
        return getValue().hashCode();
    }

    @Override
    public boolean equals(Object other) {

        if(!(other instanceof PropertyValue)) {
            return false;
        }
        PropertyValue otherValue = (PropertyValue)other;
        if(getValue() == null) {
            return otherValue.getValue() == null;
        }
        return getValue().equals(otherValue.getValue());
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }

    @Override
    public <T> T getValue(Class<T> clazz) {

        if(Enum.class.isAssignableFrom(clazz)) {
            Class<? extends Enum> enumType = (Class<? extends Enum>) clazz;
            if(value.isNumber()) {
                int ordinal = value.getAsInt();
                for(T choice : clazz.getEnumConstants()) {
                    Enum enumValue = (Enum)choice;
                    if(enumValue.ordinal() == ordinal) {
                        return (T)enumValue;
                    }
                }
            }
            else if(value.isString()) {
                String s = value.getAsString();
                return (T)Enum.valueOf(enumType, s);
            }
            throw new RuntimeException("Unknown enumeration value: " + value + " for " + clazz);
        }
        if(clazz == Number.class) {
            return (T)value.getAsNumber();
        }
        if(clazz == Boolean.class || clazz == boolean.class) {
            return (T)Boolean.valueOf(value.getAsBoolean());
        }
        if(clazz == Character.class || clazz == char.class) {
            return (T)Character.valueOf(value.getAsCharacter());
        }
        if(clazz == Byte.class || clazz == byte.class) {
            return (T)Byte.valueOf(value.getAsByte());
        }
        if(clazz == Integer.class || clazz == int.class) {
            return (T)Integer.valueOf(value.getAsInt());
        }
        if(clazz == Short.class || clazz == short.class) {
            return (T)Short.valueOf(value.getAsShort());
        }
        if(clazz == Long.class || clazz == long.class) {
            return (T)Long.valueOf(value.getAsLong());
        }
        if(clazz == Double.class || clazz == double.class) {
            return (T)Double.valueOf(value.getAsDouble());
        }
        if(clazz == Float.class || clazz == float.class) {
            return (T)Float.valueOf(value.getAsFloat());
        }
        //String->Element and String->List conversions are handled by the caller
        // See IBMGraphElement#getProperties
        if (clazz == String.class ||
                clazz == Object.class || AtlasElement.class.isAssignableFrom(clazz) || clazz == List.class) {

            return (T)value.getAsString();
        }

        if(clazz == BigDecimal.class) {
            return (T)value.getAsBigDecimal();
        }
        if(clazz == BigInteger.class) {
            return (T)value.getAsBigInteger();
        }

        //TBD - find/create appropriate subclass of AtlasException
        throw new RuntimeException("Could not convert " + value + " to " + clazz);

    }

    private Object getValueToStore() {

        Object value = getValue();

        if(value.getClass().isPrimitive()) {
            return value;
        }
        if(BigInteger.class.isAssignableFrom(value.getClass())) {
            return value.toString();
        }
        if(BigDecimal.class.isAssignableFrom(value.getClass())) {
            return value.toString();
        }
        if(Number.class.isAssignableFrom(value.getClass())) {
            return value;
        }
        if(Boolean.class.isAssignableFrom(value.getClass())) {
            return value;
        }

        return value.toString();
    }

    @Override
    public boolean isIndexed() {
        return true;
    }

    @Override
    public void addToElementChanges(String propertyName, ElementChanges changes) {
        changes.getPropertiesToSet().put(propertyName, getValueToStore());

    }
}
