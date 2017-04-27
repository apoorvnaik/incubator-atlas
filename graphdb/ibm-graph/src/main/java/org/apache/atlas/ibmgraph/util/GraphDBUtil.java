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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.IdentifierExpression;
import org.apache.atlas.groovy.StatementListExpression;
import org.apache.atlas.groovy.VariableAssignmentExpression;
import org.apache.atlas.ibmgraph.ElementDeletedCheckingInvocationHandler;
import org.apache.atlas.ibmgraph.IBMGraphEdge;
import org.apache.atlas.ibmgraph.IBMGraphElement;
import org.apache.atlas.ibmgraph.IBMGraphGraph;
import org.apache.atlas.ibmgraph.IBMGraphPropertyKey;
import org.apache.atlas.ibmgraph.IBMGraphVertex;
import org.apache.atlas.ibmgraph.TenantGraphStrategy;
import org.apache.atlas.ibmgraph.api.Cardinality;
import org.apache.atlas.ibmgraph.api.GraphDatabaseClient;
import org.apache.atlas.ibmgraph.api.GraphDatabaseConfiguration;
import org.apache.atlas.ibmgraph.api.json.Index;
import org.apache.atlas.ibmgraph.api.json.Index.IndexType;
import org.apache.atlas.ibmgraph.api.json.PropertyDataType;
import org.apache.atlas.ibmgraph.api.json.PropertyKey;
import org.apache.atlas.ibmgraph.api.json.Schema.IndexDefinition;
import org.apache.atlas.ibmgraph.exception.GraphDatabaseException;
import org.apache.atlas.ibmgraph.exception.HttpException;
import org.apache.atlas.ibmgraph.gremlin.stmt.ImportStatement;
import org.apache.atlas.ibmgraph.http.HttpResponse;
import org.apache.atlas.repository.graphdb.AtlasCardinality;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.typesystem.types.cache.DefaultTypeCache;
import org.apache.atlas.typesystem.types.cache.TypeCache;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.gson.Gson;

/**
 * Utility methods
 */
public class GraphDBUtil {


    private static final Logger LOG = LoggerFactory.getLogger(GraphDBUtil.class.getName());

    private static final Gson GSON = GraphDatabaseClient.GSON;

    public static Gson getGson() {
        return GSON;
    }

    /**
     * Converts a Multiplicity to a Cardinality.
     *
     * @param multiplicity
     * @return
     */
    public static Cardinality createCardinality(AtlasCardinality cardinality) {
        switch(cardinality) {

        case SINGLE:
            return Cardinality.SINGLE;
        case LIST:
            return Cardinality.LIST;
        case SET:
            return Cardinality.SET;
        default:
            throw new IllegalStateException("Unrecognized cardinality: " + cardinality);
        }
    }

    /**
     * Converts a Multiplicity to a Cardinality.
     *
     * @param multiplicity
     * @return
     */
    public static AtlasCardinality createCardinality(Cardinality cardinality) {
        switch(cardinality) {

        case SINGLE:
            return AtlasCardinality.SINGLE;
        case LIST:
            return AtlasCardinality.LIST;
        case SET:
            return AtlasCardinality.SET;
        default:
            throw new IllegalStateException("Unrecognized cardinality: " + cardinality);
        }
    }

    public static PropertyDataType createPropertyDataType(Class clazz) {
        if (clazz == Integer.class || clazz == Short.class || clazz == Byte.class || clazz == Long.class) {
            return PropertyDataType.Integer;
        }
        else if (clazz == Float.class || clazz == Double.class) {
            return PropertyDataType.Float;
        }
        else if (clazz == Boolean.class) {
            return PropertyDataType.Boolean;
        }
        else {
            return PropertyDataType.String;
        }
    }

    public static IdentifierExpression getGremlinDirection(AtlasEdgeDirection dir) {

        if(dir == AtlasEdgeDirection.BOTH) {
            return new IdentifierExpression("Direction.BOTH");
        }

        if(dir == AtlasEdgeDirection.IN) {
            return new IdentifierExpression("Direction.IN");
        }

        if(dir == AtlasEdgeDirection.OUT) {
            return new IdentifierExpression("Direction.OUT");
        }

        throw new GraphDatabaseException("Invalid edge direction: " + dir);
    }

    public static PropertyKey unwrapPropertyKey(AtlasPropertyKey input) {
        if(input == null) {
            return null;
        }
        return ((IBMGraphPropertyKey)input).getWrappedKey();
    }

    public static Collection<PropertyKey> unwrapPropertyKeys(Collection<AtlasPropertyKey> keys) {
        return Collections2.transform(keys, new Function<AtlasPropertyKey,PropertyKey>() {

            @Override
            public PropertyKey apply(AtlasPropertyKey input) {
                return unwrapPropertyKey(input);
            }

        });
    }

    public static List<PropertyKey> unwrapPropertyKeys(List<AtlasPropertyKey> keys) {
        return Lists.transform(keys, new Function<AtlasPropertyKey,PropertyKey>() {

            @Override
            public PropertyKey apply(AtlasPropertyKey input) {
                return unwrapPropertyKey(input);
            }

        });
    }

    public static AtlasPropertyKey wrapPropertyKey(PropertyKey input) {
        if(input == null) {
            return null;
        }
        return new IBMGraphPropertyKey(input);
    }

    public static Index convertIndexDefinition(AtlasGraphManagement mgmt, IndexType type, IndexDefinition input) {
        List<PropertyKey> keys = new ArrayList<PropertyKey>();
        for(String keyName : input.getPropertyKeys()) {
            keys.add(unwrapPropertyKey(mgmt.getPropertyKey(keyName)));
        }

        Index idx = new Index(input.getName(),
                input.isComposite(),
                input.isUnique(),
                input.requiresReindex(),
                type,
                keys);
        return idx;
    }


    public static Collection<String> getIndexNames(Collection<Index> idx) {
        return Collections2.transform(idx, new Function<Index,String>() {

            @Override
            public String apply(Index index) {
                return index.getName();
            }

        });

    }
    public static IndexDefinition convertIndex(Index input) {

        List<String> keys = input.getPropertyKeyNames();

        IndexDefinition idx = new IndexDefinition(input.getName(),
                input.isComposite(),
                input.isUnique(),
                input.requiresReindex(),
                keys);
        return idx;
    }
    private static final long MS_PER_SECOND = 1000;
    private static final long MS_PER_MINUTE = MS_PER_SECOND * 60;
    private static final long MS_PER_HOUR = MS_PER_MINUTE * 60;

    public static String formatTime(long ms) {
        long remainingMs = ms;
        long hours = remainingMs / MS_PER_HOUR;
        remainingMs -= hours * MS_PER_HOUR;
        long minutes = remainingMs / MS_PER_MINUTE;
        remainingMs -= minutes * MS_PER_MINUTE;
        long seconds = remainingMs / MS_PER_SECOND;
        remainingMs -= seconds * MS_PER_SECOND;

        if(hours == 0 && minutes == 0 && seconds == 0) {
            return ms + " ms";
        }

        if(hours == 0 && minutes == 0) {
            return String.format("%1$02d.%2$03d sec", seconds, remainingMs);
        }

        return String.format("%1$02d:%2$02d:%3$02d.%4$03d", hours, minutes, seconds, remainingMs);
    }

    public static <T> T asSingleValue(Collection<T> value) {
        if(value == null) {
            return null;
        }
        if(value.size() > 1) {
            throw new GraphDatabaseException("Too many values found in result : " + value);
        }
        if(value.size() == 0) {
            return null;
        }
        return value.iterator().next();
    }

    public static String removePrefix(String value, String prefix) {
        return value.substring(prefix.length());
    }

    public static IBMGraphElement unwrapIfNeeded(AtlasElement element) {

        if (Proxy.isProxyClass(element.getClass())) {
            InvocationHandler handler = Proxy.getInvocationHandler(element);
            if (handler instanceof ElementDeletedCheckingInvocationHandler) {
                return ((ElementDeletedCheckingInvocationHandler) handler).getWrappedElement();
            }
        }
        if (element instanceof IBMGraphElement) {
            return (IBMGraphElement) element;
        }
        throw new IllegalStateException("Unknown AtlasElement implementation: " + element.getClass());

    }

    public static AtlasVertex<IBMGraphVertex,IBMGraphEdge> addVertexProxyWrapper(AtlasVertex<IBMGraphVertex,IBMGraphEdge> element) {
        return addVertexProxyWrapper(element.getV());
    }

    public static AtlasVertex<IBMGraphVertex,IBMGraphEdge> addVertexProxyWrapper(IBMGraphVertex element) {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        return (AtlasVertex<IBMGraphVertex,IBMGraphEdge>)Proxy.newProxyInstance(classloader, new Class[]{AtlasVertex.class}, new ElementDeletedCheckingInvocationHandler(element));
    }


    public static AtlasEdge<IBMGraphVertex,IBMGraphEdge> addEdgeProxyWrapper(AtlasEdge<IBMGraphVertex,IBMGraphEdge> element) {
        return addEdgeProxyWrapper(element.getE());
    }

    public static AtlasEdge<IBMGraphVertex,IBMGraphEdge> addEdgeProxyWrapper(IBMGraphEdge element) {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        return (AtlasEdge<IBMGraphVertex,IBMGraphEdge>)Proxy.newProxyInstance(classloader, new Class[]{AtlasEdge.class}, new ElementDeletedCheckingInvocationHandler(element));
    }

    public static Collection<AtlasEdge<IBMGraphVertex,IBMGraphEdge>> addEdgeProxyWrappers(Collection<? extends AtlasEdge<IBMGraphVertex,IBMGraphEdge>> edges) {
        return Collections2.transform(edges, new Function<AtlasEdge<IBMGraphVertex,IBMGraphEdge>,AtlasEdge<IBMGraphVertex,IBMGraphEdge>>() {

            @Override
            public AtlasEdge<IBMGraphVertex,IBMGraphEdge> apply(AtlasEdge<IBMGraphVertex,IBMGraphEdge> input) {
                return addEdgeProxyWrapper(input);
            }

        });
    }

    public static Collection<AtlasVertex<IBMGraphVertex,IBMGraphEdge>> addVertexProxyWrappers(Collection<? extends AtlasVertex<IBMGraphVertex,IBMGraphEdge>> vertices) {
        return Collections2.transform(vertices, new Function<AtlasVertex<IBMGraphVertex,IBMGraphEdge>,AtlasVertex<IBMGraphVertex,IBMGraphEdge>>() {

            @Override
            public AtlasVertex<IBMGraphVertex,IBMGraphEdge> apply(AtlasVertex<IBMGraphVertex,IBMGraphEdge> input) {
                return addVertexProxyWrapper(input);
            }

        });
    }
    public static void logInfoMessage (IBMGraphGraph graph, String message, Logger LOG)
    {
        LOG.info( "{} :'{}' : message", graph.getGraphId(), graph.getTx().getId());
    }

    public static void logDebugMessage (IBMGraphGraph graph, String message, Logger LOG)
    {
        if (LOG.isDebugEnabled())
        {
            LOG.debug( "{} :'{}' : message", graph.getGraphId(), graph.getTx().getId());
        }
    }
    public static final String IBM_GRAPH_REQUEST_ID_HEADER = "Request-Id";

    /**
     * Creates script that adds the required imports and variable definitions
     * before executing the given statement.
     *
     * @param tenantId
     * @param originalStmt
     * @return
     */
    public static StatementListExpression addDefaultInitialStatements(String tenantId, GroovyExpression originalStmt) {
        StatementListExpression stmt = new StatementListExpression();
        stmt.addStatement(new ImportStatement("java.util.function.Function"));
        stmt.addStatement(new ImportStatement("org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategy"));
        stmt.addStatement(new ImportStatement(true, "org.apache.tinkerpop.gremlin.process.traversal.P.*"));
        TenantGraphStrategy strategy = GraphDatabaseConfiguration.INSTANCE.getTenantGraphStrategy();
        stmt.addStatement(new VariableAssignmentExpression("g", strategy.getGraphTraversalExpression(tenantId)));
        stmt.addStatement(originalStmt);
        return stmt;
    }

    public static boolean responseContains(HttpException e, String text) {
        HttpResponse response = e.getResponse();
        if(response != null) {
            String responseBody = response.getResponseBody();
            if(responseBody != null && responseBody.contains(text)) {
                return true;
            }
        }
        return false;

    }
    public static final String TYPE_CACHE_IMPLEMENTATION_PROPERTY = "atlas.TypeCache.impl";
    public static Class<? extends TypeCache> getTypeCache() {

        // Get the type cache implementation class from Atlas configuration.
        try {
            Configuration config = ApplicationProperties.get();
            return ApplicationProperties.getClass(config, TYPE_CACHE_IMPLEMENTATION_PROPERTY,
                    DefaultTypeCache.class.getName(), TypeCache.class);
        } catch (AtlasException e) {
            return DefaultTypeCache.class;
        }
    }
}
