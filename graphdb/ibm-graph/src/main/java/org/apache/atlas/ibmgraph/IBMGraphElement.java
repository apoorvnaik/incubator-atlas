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

package org.apache.atlas.ibmgraph;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.atlas.ibmgraph.api.IGraphDatabaseClient;
import org.apache.atlas.ibmgraph.api.action.ElementIdListPropertyValue;
import org.apache.atlas.ibmgraph.api.action.ElementIdPropertyValue;
import org.apache.atlas.ibmgraph.api.action.IPropertyValue;
import org.apache.atlas.ibmgraph.api.json.JsonGraphElement;
import org.apache.atlas.ibmgraph.api.json.PropertyValue;
import org.apache.atlas.ibmgraph.graphson.AtlasGraphSONMode;
import org.apache.atlas.ibmgraph.graphson.AtlasGraphSONUtility;
import org.apache.atlas.ibmgraph.gremlin.expr.GetElementExpression.ElementType;
import org.apache.atlas.ibmgraph.tx.DefaultUpdatedGraphElement;
import org.apache.atlas.ibmgraph.tx.ElementBackedPropertyMap;
import org.apache.atlas.ibmgraph.tx.ReadableUpdatedGraphElement;
import org.apache.atlas.ibmgraph.tx.UpdatedGraphElement;
import org.apache.atlas.ibmgraph.util.AllowedWhenDeleted;
import org.apache.atlas.ibmgraph.util.GraphDBUtil;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;

/**
 * Represents an element in IBM Graph.
 */
public abstract class IBMGraphElement<T extends JsonGraphElement> implements AtlasElement {

    private static final String HEX_PREFIX = (char)2 + "hex:";

    public static final String LIST_PREFIX = (char)2 + "list:";
    public static final String LIST_DELIMITER = String.valueOf((char)3);

    protected final IBMGraphGraph graph_;
    protected volatile String elementId_;

    //temporary identifier for new elements
    protected final String localElementId_;

    protected abstract ElementType getElementType();
    protected abstract T loadJsonInfo(String id);

    //creates new element and assign id
    protected abstract void createNewElement();
    protected abstract UpdatedGraphElement getUpdatedElement();
    protected abstract ReadableUpdatedGraphElement getReadOnlyUpdatedElement();

    //whether this is a newly created element that has not been pushed into the graph yet.  Volatile
    //so that other threads immediately see the value when it changes.
    private volatile State elementState_ = null;

    private volatile Map<String,Set<IPropertyValue>> committedPropertyValues_ = null;

    private volatile String label_;

    private static final Logger LOG = LoggerFactory.getLogger(IBMGraphElement.class);

    //save the hashCode so it does not change when the element id is assigned.  This is needed because
    //some caches use the Element as the key (especially for new elements/vertices) when the id is not
    //assigned yet.
    private final int hashCode_;

    private enum State {
        NEW,
        CREATING, //transient state for thread communication
        PROXY,
        RESOLVING_PROXY, //transient state for thread communication
        NORMAL,
    }

    private volatile boolean isDeletedInGraph_ = false;

    /**
     * Constructor for new elements
     *
     * @param graph
     * @param label
     */
    public IBMGraphElement(IBMGraphGraph graph, String localId, String label, Map<String,Set<IPropertyValue>> initialProperties) {

        label_ = label;
        graph_ = graph;
        localElementId_ = localId;
        elementState_ = State.NEW;
        hashCode_ = computeHashCode();
        committedPropertyValues_ = new HashMap<>();

        getUpdatedElement().getPropertyValues().putAll(initialProperties);

        if(LOG.isTraceEnabled()) {
            LOG.trace("Created NEW element {} with label {}", toString(), label, new Exception());
        }

    }

    /**
     * Constructor for elements loaded from the graph
     * @param graph
     * @param element
     */
    public IBMGraphElement(IBMGraphGraph graph, T element) {
        if(element == null) {
            throw new NullPointerException();
        }
        graph_ = graph;
        elementState_ = State.NORMAL;
        setFieldsFromLoadedInfo(element);
        hashCode_ = computeHashCode();
        if(LOG.isTraceEnabled()) {
            LOG.trace("Created NORMAL element {} for {}", toString(), element, new Exception());
        }
        localElementId_ = null;
    }

    /**
     * Constructor for elements created as a proxy.
     *
     * @param graph
     * @param vertexId
     */
    public IBMGraphElement(IBMGraphGraph graph, String elementId) {
        if(elementId == null) {
            throw new NullPointerException();
        }
        elementId_ = elementId;
        graph_ = graph;
        elementState_ = State.PROXY;
        hashCode_ = computeHashCode();
        if(LOG.isTraceEnabled()) {
            LOG.trace("Created PROXY element {} for {}", toString(), elementId, new Exception());
        }
        localElementId_ = null;
    }

    private Map<String, Set<IPropertyValue>> getPropertyValues() {

        resolveProxyIfNeeded(true);
        return getUpdatedElement().getPropertyValues();
    }

    @AllowedWhenDeleted
    public String getLabel() {

        if(label_ == null) {
            resolveProxyIfNeeded(true);
        }
        return label_;
    }

    /**
     *
     * @param failIfNotExists
     */
    protected void resolveProxyIfNeeded(boolean failIfNotExists) {
        synchronized(this) {

            waitForProxyResolutionToComplete();

            if(isDeleted()) {
                return;
            }

            if(!isProxy()) {
                return;
            }

            elementState_ = State.RESOLVING_PROXY;
        }

        try {

            LOG.debug(Thread.currentThread().getName() + ": Resolving proxy {}", toString());
            //resolve the proxy outside of the synchronized block

            T loadJsonInfo = loadJsonInfo(getIdString());

            if(loadJsonInfo == null) {
                delete();

                //element is not in the graph, mark it deleted globally
                markDeleted();

                if(failIfNotExists) {
                    throw new IllegalStateException("No element with id " + elementId_ + " exists");
                }
                return;
            }
            setFieldsFromLoadedInfo(loadJsonInfo);

        }
        finally {
            //make sure that notify always gets called to
            LOG.debug(Thread.currentThread().getName() + ": Done resolving proxy {}", toString());
            synchronized(this) {
                elementState_ = State.NORMAL;
                notifyAll();
            }


        }
    }
    private void waitForProxyResolutionToComplete() {
        synchronized(this) {
            while(elementState_ == State.RESOLVING_PROXY) {
                //wait for proxy resolution to complete
                try {
                    if(LOG.isDebugEnabled()) {
                        LOG.debug(Thread.currentThread().getName() + ": waiting for proxy resolution for {}", toString());
                    }
                    wait();
                    if(LOG.isDebugEnabled()) {
                        LOG.debug(Thread.currentThread().getName() + ": Done waiting for proxy resolution for {}", toString());
                    }
                }
                catch(InterruptedException e) {

                }
            }
        }
    }


    protected void setFieldsFromLoadedInfo(T loadedInfo) {
        elementId_ = loadedInfo.getId();
        label_ = loadedInfo.getLabel();
        Map loaded = loadedInfo.getProperties();
        committedPropertyValues_ = new HashMap<>(loaded.size());
        committedPropertyValues_.putAll(loaded);

    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.atlas.repository.graphdb.AtlasElement#removeProperty(java.lang
     * .String)
     */
    @Override
    public void removeProperty(String propertyName) {

        if(LOG.isDebugEnabled()) {
            LOG.debug("{}: Removing property {}", toString(), propertyName);
        }
        Collection oldValues = getProperties(Object.class).get(propertyName);
        graph_.getPropertyChangeIndex().onPropertyRemove(this, propertyName, oldValues);
        getPropertyValues().remove(propertyName);
    }

    @Override
    public final <T> void setProperty(String propertyName, T value) {

        Object pv = getPersistentPropertyValue(value);
        PropertyValue propertyValue = makePropertyValue(pv);
        setProperty_(propertyName, propertyValue);
    }

    protected <T> PropertyValue makePropertyValue(T value) {
        PropertyValue propertyValue = new PropertyValue(null, value);
        return propertyValue;
    }

    protected void updatePropertyValueInMap(String property, IPropertyValue value) {
        if(value.isIndexed()) {
            Object oldValue = getProperty(property, value.getOriginalType());
            Object newValue = value.getValue(value.getOriginalType());
            graph_.getPropertyChangeIndex().onPropertySet(this, property, oldValue, newValue);
        }
        getUpdatedElement().replacePropertyValueInMap(property, value);
    }

    //sets a property using a pre-computed 'persistent' value
    private void setProperty_(String propertyName, IPropertyValue propertyValue) {

        if(LOG.isDebugEnabled()) {
            LOG.debug("{}: Setting property {} to {}", toString(), propertyName, propertyValue);
        }

        updatePropertyValueInMap(propertyName, propertyValue);
    }


    @Override
    public final <T> void setJsonProperty(String propertyName, T value) {

        String hexEncodedJson = hexEncode(value.toString());
        PropertyValue pv = makePropertyValue(hexEncodedJson);
        setProperty_(propertyName, pv);
    }

    @Override
    public <T> T getJsonProperty(String propertyName) {

        String value = getProperty(propertyName, String.class);
        T decodedJson = (T)getTransformedPropertyValue(value, String.class);
        return (T)decodedJson;

    }

    /**
     * Gets all of the values of the given property.
     * @param propertyName
     * @return
     */
    @Override
    public <T> Collection<T> getPropertyValues(String propertyName, Class<T> type) {

        return Collections.singleton(getProperty(propertyName, type));
    }

    @Override
    public <V> V getProperty(String propertyName, Class<V> clazz) {

        if(LOG.isDebugEnabled()) {
            LOG.debug("{}: Getting value of property {}  with class {}", toString(), propertyName, clazz.getName());
        }

        Map<String, Set<IPropertyValue>> props = getReadOnlyPropertyValues();

        Set<IPropertyValue> toTransform = props.get(propertyName);
        if(toTransform == null) {
            return null;
        }
        Collection<V> values = convertPropertyValues(toTransform, clazz);
        if(values.size() == 0) {
            return null;
        }
        if(values.size() > 1) {
            throw new IllegalStateException("Multiple property values exist.  Use getPropertyValues()");
        }
        return (V)values.iterator().next();
    }



    private <U> Collection<U> convertPropertyValues(Set<IPropertyValue> toConvert, Class<U> clazz) {

        if(toConvert.size() == 1) {
            IPropertyValue value = toConvert.iterator().next();
            U storedPropertyValue = value.getValue(clazz);

            if(storedPropertyValue == null) {
                return Collections.emptyList();
            }

            U propertyValueToReturn = getTransformedPropertyValue_(graph_, storedPropertyValue, clazz);
            return Collections.<U>singleton(propertyValueToReturn);
        }


        Collection<U> converted =  Collections2.transform(toConvert, new Function<IPropertyValue, U>() {

            @Override
            public U apply(IPropertyValue in) {

                U stored =  in.getValue(clazz);
                if(stored == null) {
                    return null;
                }
                return getTransformedPropertyValue_(graph_, stored, clazz);
            }
        });

        //filter out the null values
        return Collections2.filter(converted,new Predicate<U>() {

            @Override
            public boolean apply(U input) {
                return input != null;
            }

        });
    }

    protected IGraphDatabaseClient getClient() {
        return graph_.getClient();
    }

    @Override
    public Collection<? extends String> getPropertyKeys() {

        Map<String,Collection<Object>> properties = getProperties(Object.class);
        return properties.keySet();
    }

    @Override
    public JSONObject toJson(Set<String> propertyKeys) throws JSONException {

        return AtlasGraphSONUtility.jsonFromElement(this, propertyKeys, AtlasGraphSONMode.NORMAL);

    }

    protected Object getPersistentPropertyValue(Object value) {

        if(value == null) {
            return null;
        }
        if(value instanceof Long) {
            //IBM Graph does not directly support long values.  They will
            //stored as integers, and values outside the integer range will
            //become unusable.  Store these as Strings.
            return String.valueOf(value);
        }
        if(value instanceof Double) {
            //IBM Graph does not directly support double values.  They will
            //stored as integers, and values outside the integer range will
            //become unusable.  Store these as Strings.
            return String.valueOf(value);
        }
        if(value instanceof Number) {
            return value;
        }

        if(value instanceof Boolean) {
            return value;
        }

        if(value instanceof List) {
            return getPersistentListValue((List)value);

        }

        //everything else needs to be stored as a String.  IBM Graph only natively
        //supports Integer, Float, and String property values.  Let gson handle
        //escaping the json values if that's needed.
        String stringValue = String.valueOf(value);
        return stringValue;
    }

    private String getPersistentListValue(List<?> list) {
        StringBuilder result = new StringBuilder();
        result.append(LIST_PREFIX);
        Iterator<?> it = list.iterator();
        while(it.hasNext()) {
            String value = String.valueOf(getPersistentPropertyValue(it.next()));
            result.append(value);
            if(it.hasNext()) {
                result.append(LIST_DELIMITER);
            }
        }

        return result.toString();
    }

    private static List getPersistedListValue(String list) {
        String listPart = GraphDBUtil.removePrefix(list, LIST_PREFIX);
        if(listPart.isEmpty()) {
            return Collections.emptyList();
        }
        String[] parts = listPart.split(LIST_DELIMITER);
        List<Object> result = new ArrayList<>(parts.length);
        for(String part : parts) {
            result.add(part);
        }
        return result;
    }

    private static Object convertValueToRequestedType(IBMGraphGraph graph, Object toConvert, Class clazz) {
        if(AtlasEdge.class == clazz) {
            return graph.getEdge(String.valueOf(toConvert));
        }
        if(AtlasVertex.class == clazz) {
            return graph.getVertex(String.valueOf(toConvert));
        }
        return toConvert;
    }

    private <T> T getTransformedPropertyValue(T found, Class<T> clazz) {
        return getTransformedPropertyValue_(graph_, found, clazz);
    }

    private static <T> T getTransformedPropertyValue_(IBMGraphGraph graph, T found, Class<T> clazz) {
        if(!(found instanceof String)) {
            //existing conversion logic only works on String and Lists of String
            return found;
        }
        try {
            Object decoded = null;
            String stringValue = (String)found;
            if(stringValue.startsWith(HEX_PREFIX)) {
                String hexPart = GraphDBUtil.removePrefix(stringValue, HEX_PREFIX);
                Hex hexCoder = new Hex();
                decoded = new String(hexCoder.decode(hexPart.getBytes()));
            }
            else if(stringValue.startsWith(LIST_PREFIX)) {
                decoded = getPersistedListValue(stringValue);
            }
            else {
                decoded = stringValue;
            }
            return (T)convertStoredValueToReturnType(graph, decoded, clazz);
        }
        catch(DecoderException e) {
            return found;
        }
    }

    private static Object convertStoredValueToReturnType(IBMGraphGraph graph, Object decoded, final Class clazz) {

        if(decoded instanceof List) {

            List originalList = (List)decoded;
            List<Object> result = new ArrayList<>(originalList.size());
            for(Object o : originalList) {

                Object converted = convertValueToRequestedType(graph, o, clazz);
                if(converted == null) {

                    LOG.warn("{}: Filtering out null value from conversion of {} to type {}", graph.toString(), decoded, clazz.getName());
                }
                if(converted != null) {

                    result.add(converted);
                }
            }
            return result;
        }
        else {
            return convertValueToRequestedType(graph, decoded, clazz);
        }
    }

    @Override
    @AllowedWhenDeleted
    public Object getId() {

        return getIdString();
    }

    protected <U> Map<String, Collection<U>> getProperties(final Class<U> clazz) {

        Map<String, Set<IPropertyValue>> from =
                getReadOnlyPropertyValues();

        Function<Set<IPropertyValue>, Collection<U>> f = new Function<Set<IPropertyValue>, Collection<U>>() {

            @Override
            public Collection<U> apply(Set<IPropertyValue> values) {
                return convertPropertyValues(values, clazz);
            }
        };

        return Maps.transformValues(from, f);
    }


    public Map<String, Set<IPropertyValue>> getReadOnlyPropertyValues() {
        ReadableUpdatedGraphElement readOnlyElement = getReadOnlyUpdatedElement();
        if(readOnlyElement == null) {
            return getReadOnlyCommittedProperties();
        }
        return readOnlyElement.getReadOnlyPropertyValues();
    }

  public <U> U transformPropertyValue(IPropertyValue value, Class<U> clazz) {
        U storedPropertyValue = value.getValue(clazz);
        return getTransformedPropertyValue(storedPropertyValue, clazz);
    }

    @Override
    public List<String> getListProperty(String propertyName) {

        Object value = getProperty(propertyName, String.class);
        if(value == null) {
            return Collections.emptyList();
        }
        return (List)value;

    }

    @Override
    public <T> List<T> getListProperty(String propertyName, Class<T> clazz) {

        Object value = getProperty(propertyName, clazz);
        if(value == null) {
            return Collections.emptyList();
        }
        return (List)value;

    }

    @Override
    public void setListProperty(String propertyName, List<String> values) {

        String persistentValue = getPersistentListValue(values);
        PropertyValue pv = makePropertyValue(persistentValue);
        setProperty_(propertyName, pv);
    }

    private static String hexEncode(String toEncode) {
        Hex hexCoder = new Hex();
        String hexEncodedJson = new String(hexCoder.encode(toEncode.getBytes()));
        return HEX_PREFIX + hexEncodedJson;
    }

    @Override
    @AllowedWhenDeleted
    public boolean exists() {

        resolveProxyIfNeeded(false);

        return ! isDeleted();

    }
    /**
     * Whether or not this is a proxy.  This will return true if the element
     * is new and is NOT in the process of being inflated.
     * @return
     */
    public boolean isProxy() {
        return elementState_  == State.PROXY;
    }

    /**
     * Whether or not this is a proxy element which is in the process of being
     * inflated.
     * @return
     */
    public boolean isResolvingProxy() {
        return elementState_  == State.RESOLVING_PROXY;
    }


    /**
     * Whether or not this is a new element.  This will return true if the element
     * is new and is NOT in the process of being created.
     * @return
     */
    public boolean isNewElement() {
        return elementState_ == State.NEW;
    }

    public boolean isBeingCreated() {
        return elementState_ == State.CREATING;
    }

    protected String getIdString() {

        synchronized (this) {
            while(elementState_ == State.CREATING) {
                //wait for element creation to complete
                try {
                    if(LOG.isDebugEnabled()) {
                        LOG.debug(Thread.currentThread().getName() + ": Waiting for the element {} to be created", toString());
                    }
                    wait();
                    if(LOG.isDebugEnabled()) {
                        LOG.debug(Thread.currentThread().getName() + ": Done waiting for the element {} to be created", toString());
                    }
                }
                catch(InterruptedException e) {

                }
            }

            if(isIdAssigned()) {
                return elementId_;
            }

            if(isDeleted()) {
                return null;
            }

            elementState_ = State.CREATING;
        }

        try {

            LOG.debug(Thread.currentThread().getName() + ": Creating element {}", toString());

            //no id yet.  Since id is required, create the element in the graph now
            //do not replace existing json info -- that has the updated property values in it!
            createNewElement();

            return elementId_;
            //change the state after creating the new element (otherwise the logic there will not think the element is new)

        }
        finally {
            LOG.debug(Thread.currentThread().getName() + ": Done creating element {}", toString());
            synchronized(this) {
                elementState_ = State.NORMAL;
                notifyAll();
            }
        }

    }

    public void assignId(String id) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("{}: Setting id to {}", toString(), id);
        }

        //synchronization is not needed here.  This is only called by code
        //that is already thread safe.
        elementState_ = State.NORMAL;
        elementId_ = id;
        graph_.onIdAssigned(this);
    }

    protected boolean isMultiProperty(String propertyName) {
        return false;
    }

    @Override
    @AllowedWhenDeleted
    public String getIdForDisplay() {

        if(! isIdAssigned()) {
            return "<unassigned>";
        }
        else {
            return getIdString();
        }
    }

    void delete() {
        LOG.debug("{}: Setting deleted flag to true", toString());
        getUpdatedElement().delete();

    }

    //for book keeping, indicates if vertex has been deleted, either in the graph
    //outside the scope of this transaction or as part if this transaction.
    public boolean isDeleted() {


        if(isDeletedInGraph_) {
            return true;
        }

        ReadableUpdatedGraphElement updatedElement = getReadOnlyUpdatedElement();
        if(updatedElement == null) {
            return false;
        }
        return updatedElement.isDeleted();
    }

    @Override
    public void setPropertyFromElementsIds(String propertyName, List<AtlasElement> values) {

        ElementIdListPropertyValue propertyValue = new ElementIdListPropertyValue(values);
        setProperty_(propertyName, propertyValue);
    }

    @Override
    public void setPropertyFromElementId(String propertyName, AtlasElement value) {


        ElementIdPropertyValue propertyValue = new ElementIdPropertyValue(value);
        setProperty_(propertyName, propertyValue);

    }

    @Override
    public int hashCode() {
        return hashCode_;
    }

    private int computeHashCode() {

        int result = 37;
        result = 17*result + getClass().hashCode();
        if(! isIdAssigned()) {
            result = 17*result + super.hashCode();
        }
        else {
            result = 17*result + getId().hashCode();
        }
        return result;
    }

    @Override
    @AllowedWhenDeleted
    public boolean equals(Object other) {

        if(other == null) {
            return false;
        }

        Object toCheck = other;

        if (Proxy.isProxyClass(other.getClass())) {
            InvocationHandler handler = Proxy.getInvocationHandler(other);
            if (handler instanceof ElementDeletedCheckingInvocationHandler) {
                toCheck = ((ElementDeletedCheckingInvocationHandler) handler).getWrappedElement();
            }
        }

        if(toCheck.getClass() != getClass()) {
            return false;
        }

        IBMGraphElement otherElement = (IBMGraphElement) toCheck;
        if( ! isIdAssigned() || ! otherElement.isIdAssigned()) {
            return this == toCheck;
        }
        return getId().equals(otherElement.getId());
    }

    @Override
    @AllowedWhenDeleted
    public String toString() {

        //include object id in toString to aid in phantom debugging.  That is the only way we
        //can detect phantoms.
        return getClass().getSimpleName() + "@" + Integer.toHexString(hashCode()) + ":" + getIdOrLocalId() + " [state="
        + elementState_
        + ", deleted=" + isDeleted() + ", id=" + getIdForDisplay()
        + (localElementId_ != null ? ", localElementId = " + localElementId_ : "")
        + (label_ != null ? ", label = " + label_ : "") + "]@" + graph_.toString();

    }

    @Override
    @AllowedWhenDeleted
    public boolean isIdAssigned() {
        return ! ( elementState_ == State.NEW || elementState_ == State.CREATING );
    }

    public Map<String, Set<IPropertyValue>> getReadOnlyCommittedProperties() {
        resolveProxyIfNeeded(true);
        return Collections.unmodifiableMap(committedPropertyValues_);
    }

    public Set<String> getCommittedPropertyNames() {
        resolveProxyIfNeeded(true);
        return committedPropertyValues_.keySet();
    }

    public Set<IPropertyValue> getCommittedPropertyValues(Object key) {
        resolveProxyIfNeeded(true);
        return committedPropertyValues_.get(key);
    }

    private void ensureFullyInflated() {
        waitForProxyResolutionToComplete();
    }

    /**
     * Called as part of committing a transaction to apply the changes that
     * were made within the transaction to the this element so that
     * other transactions will see them and they are preserved after the
     * transaction object has been discarded.
     *
     * @param element
     */
    public void applyChangesFromTransaction(DefaultUpdatedGraphElement element) {

        //don't force the proxy to be resolved just to update the property values.  The
        //latest values will be picked up from IBM Graph when (/if) the proxy is inflated.

        if(isDeletedInGraph()) {
            //Element was deleted by some other transaction.  Don't apply the changes.
            return;
        }

        if(! isProxy()) {

            ensureFullyInflated();
            ElementBackedPropertyMap updatedProperties = element.getPropertyValues();
            updatedProperties.applyChanges(committedPropertyValues_);
        }

        if(isDeleted()) {
            markDeleted();
        }
    }

    public void markDeleted() {
        isDeletedInGraph_ = true;
    }

    public boolean isDeletedInGraph() {
        return isDeletedInGraph_;
    }

    public String getIdOrLocalId() {
        if (isIdAssigned()) {
            return getIdString();
        } else {
            return localElementId_;
        }
    }

}
