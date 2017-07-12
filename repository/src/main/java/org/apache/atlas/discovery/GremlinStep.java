/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.discovery;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.discovery.SearchParameters.FilterCriteria;
import org.apache.atlas.model.discovery.SearchParameters.FilterCriteria.Condition;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphTraversal;
import org.apache.atlas.repository.graphdb.AtlasGraphTraversal.ComparisonOp;
import org.apache.atlas.repository.graphdb.AtlasGraphTraversal.PatternOp;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v1.AtlasGraphUtilsV1;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.apache.atlas.discovery.SearchPipeline.IndexResultType;
import static org.apache.atlas.discovery.SearchPipeline.PipelineContext;
import static org.apache.atlas.discovery.SearchPipeline.PipelineStep;

@Component
public class GremlinStep implements PipelineStep {
    private static final Logger LOG      = LoggerFactory.getLogger(GremlinStep.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("GremlinStep");

    private final AtlasGraph graph;

    enum GremlinFilterQueryType { TAG, ENTITY }

    @Inject
    public GremlinStep(AtlasGraph graph) {
        this.graph = graph;
    }

    @Override
    public void execute(PipelineContext context) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> GremlinStep.execute({})", context);
        }

        if (context == null) {
            throw new AtlasBaseException("Can't start search without any context");
        }

        AtlasPerfTracer perf = null;

        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GremlinSearchStep.execute(" + context +  ")");
        }

        final Iterator<AtlasVertex> result;

        if (context.hasIndexResults()) {
            // We have some results from the indexed step, let's proceed accordingly
            if (context.getIndexResultType() == IndexResultType.TAG) {
                // Index search was done on tag and filters
                if (context.isTagProcessingComplete()) {
                    LOG.debug("GremlinStep.execute(): index has completely processed tag, further TAG filtering not needed");

                    Set<String> taggedVertexGUIDs = new HashSet<>();

                    Iterator<AtlasIndexQuery.Result> tagVertexIterator = context.getIndexResultsIterator();

                    while (tagVertexIterator.hasNext()) {
                        // Find out which Vertex has this outgoing edge
                        AtlasVertex         vertex = tagVertexIterator.next().getVertex();
                        Iterable<AtlasEdge> edges  = vertex.getEdges(AtlasEdgeDirection.IN);

                        for (AtlasEdge edge : edges) {
                            String guid = AtlasGraphUtilsV1.getIdFromVertex(edge.getOutVertex());

                            taggedVertexGUIDs.add(guid);
                        }
                    }

                    // No entities are tagged  (actually this check is already done)
                    if (!taggedVertexGUIDs.isEmpty()) {
                        result = processEntity(taggedVertexGUIDs, context);
                    } else {
                        result = null;
                    }
                } else {
                    result = processTagAndEntity(Collections.<String>emptySet(), context);
                }
            } else if (context.getIndexResultType() == IndexResultType.TEXT) {
                // Index step processed full-text;
                Set<String> entityIDs = getVertexIDs(context.getIndexResultsIterator());

                result = processTagAndEntity(entityIDs, context);
            } else if (context.getIndexResultType() == IndexResultType.ENTITY) {
                // Index step processed entity and it's filters; tag filter wouldn't be set
                Set<String> entityIDs = getVertexIDs(context.getIndexResultsIterator());

                result = processEntity(entityIDs, context);
            } else {
                result = null;
            }
        } else {
            // No index results, need full processing in Gremlin
            if (context.getClassificationType() != null) {
                // Process tag and filters first, then entity filters
                result = processTagAndEntity(Collections.<String>emptySet(), context);
            } else {
                result = processEntity(Collections.<String>emptySet(), context);
            }
        }

        context.setGremlinResultIterator(result);

        AtlasPerfTracer.log(perf);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== GremlinStep.execute({})", context);
        }
    }

    private Iterator<AtlasVertex> processEntity(Set<String> entityGUIDs, PipelineContext context) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> GremlinStep.processEntity(entityGUIDs={})", entityGUIDs);
        }

        final Iterator<AtlasVertex> ret;

        SearchParameters searchParameters = context.getSearchParameters();
        AtlasEntityType  entityType       = context.getEntityType();

        if (entityType != null) {
            AtlasGraphTraversal entityFilterTraversal = context.getGraphTraversal("ENTITY_FILTER");

            if (entityFilterTraversal == null) {
                entityFilterTraversal = graph.traversal()
                        .V().has(Constants.TYPE_NAME_PROPERTY_KEY, ComparisonOp.IN, entityType.getTypeAndAllSubTypes());

                if (searchParameters.getEntityFilters() != null) {
                    toGremlinTraversal(GremlinFilterQueryType.ENTITY, entityType, searchParameters.getEntityFilters(), entityFilterTraversal, context);
                }

                if (searchParameters.getExcludeDeletedEntities()) {
                    entityFilterTraversal.has(Constants.STATE_PROPERTY_KEY, "ACTIVE");
                }

                context.cacheGraphTraversal("ENTITY_FILTER", entityFilterTraversal);
            }

            // Now get all vertices
            if (CollectionUtils.isEmpty(entityGUIDs)) {
                ret = entityFilterTraversal.range(context.getCurrentOffset(), context.getMaxLimit()).toList().iterator();
            } else {
                AtlasGraphTraversal guidQuery = entityFilterTraversal.has(Constants.GUID_PROPERTY_KEY, ComparisonOp.IN, entityGUIDs);

                if (entityFilterTraversal != null) {
                    guidQuery.and(entityFilterTraversal);
                } else if (searchParameters.getExcludeDeletedEntities()) {
                    guidQuery.has(Constants.STATE_PROPERTY_KEY, "ACTIVE");
                }

                ret = guidQuery.range(context.getCurrentOffset(), context.getMaxLimit()).toList().iterator();
            }
        } else if (CollectionUtils.isNotEmpty(entityGUIDs)) {
            AtlasGraphTraversal guidQuery = graph.traversal().has(Constants.GUID_PROPERTY_KEY, ComparisonOp.IN, entityGUIDs);

            if (searchParameters.getExcludeDeletedEntities()) {
                guidQuery.has(Constants.STATE_PROPERTY_KEY, "ACTIVE");
            }

            ret = guidQuery.range(context.getCurrentOffset(), context.getMaxLimit()).toList().iterator();
        } else {
            ret = null;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== GremlinStep.processEntity(entityGUIDs={})", entityGUIDs);
        }

        return ret;
    }

    private Iterator<AtlasVertex> processTagAndEntity(Set<String> entityGUIDs, PipelineContext context) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> GremlinStep.processTagAndEntity(entityGUIDs={})", entityGUIDs);
        }

        final Iterator<AtlasVertex> ret;

        AtlasClassificationType classificationType = context.getClassificationType();

        if (classificationType != null) {
            AtlasGraphTraversal tagVertexQuery = context.getGraphTraversal("TAG_VERTEX");

            if (tagVertexQuery == null) {
                tagVertexQuery = graph.traversal().has(Constants.TYPE_NAME_PROPERTY_KEY, ComparisonOp.IN, classificationType.getTypeAndAllSubTypes());

                SearchParameters searchParameters = context.getSearchParameters();

                // Do tag filtering first as it'll return a smaller subset of vertices
                if (searchParameters.getTagFilters() != null) {
                    toGremlinTraversal(GremlinFilterQueryType.TAG, classificationType, searchParameters.getTagFilters(), tagVertexQuery, context);
                }

                context.cacheGraphTraversal("TAG_VERTEX", tagVertexQuery);
            }

            if (tagVertexQuery != null) {
                Set<String> taggedVertexGuids = new HashSet<>();
                // Now get all vertices after adjusting offset for each iteration
                LOG.debug("Firing TAG query");

                Iterator<AtlasVertex> tagVertexIterator = tagVertexQuery.range(context.getCurrentOffset(), context.getMaxLimit()).toList().iterator();

                while (tagVertexIterator.hasNext()) {
                    // Find out which Vertex has this outgoing edge
                    Iterable<AtlasEdge> edges = tagVertexIterator.next().getEdges(AtlasEdgeDirection.IN);
                    for (AtlasEdge edge : edges) {
                        String guid = AtlasGraphUtilsV1.getIdFromVertex(edge.getOutVertex());
                        taggedVertexGuids.add(guid);
                    }
                }

                entityGUIDs = taggedVertexGuids;
            }
        }

        if (!entityGUIDs.isEmpty()) {
            ret = processEntity(entityGUIDs, context);
        } else {
            ret = null;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== GremlinStep.processTagAndEntity(entityGUIDs={})", entityGUIDs);
        }

        return ret;
    }

    private Set<String> getVertexIDs(Iterator<AtlasIndexQuery.Result> idxResultsIterator) {
        Set<String> guids = new HashSet<>();
        while (idxResultsIterator.hasNext()) {
            AtlasVertex vertex = idxResultsIterator.next().getVertex();
            String guid = AtlasGraphUtilsV1.getIdFromVertex(vertex);
            guids.add(guid);
        }
        return guids;
    }

    private Set<String> getVertexIDs(Iterable<AtlasVertex> vertices) {
        Set<String> guids = new HashSet<>();
        for (AtlasVertex vertex : vertices) {
            String guid = AtlasGraphUtilsV1.getIdFromVertex(vertex);
            guids.add(guid);
        }
        return guids;
    }

    private AtlasGraphTraversal toGremlinTraversal(GremlinFilterQueryType queryType, AtlasStructType type, FilterCriteria criteria,
                                                   AtlasGraphTraversal traversal, PipelineContext context) {
        if (criteria.getCondition() != null) {
            if (criteria.getCondition() == Condition.AND) {
                for (FilterCriteria filterCriteria : criteria.getCriterion()) {
                    AtlasGraphTraversal nestedTraversal = toGremlinTraversal(queryType, type, filterCriteria, graph.traversal(), context);
                    traversal.and(nestedTraversal);
                }
            } else {
                for (FilterCriteria filterCriteria : criteria.getCriterion()) {
                    AtlasGraphTraversal nestedTraversal = toGremlinTraversal(queryType, type, filterCriteria, graph.traversal(), context);
                    traversal.or(graph.traversal().and(nestedTraversal));
                }
            }
        } else {
            String   attrName  = criteria.getAttributeName();
            String   attrValue = criteria.getAttributeValue();
            SearchParameters.Operator operator  = criteria.getOperator();

            try {
                // If attribute belongs to supertype then adjust the name accordingly
                final String  qualifiedAttributeName = type.getQualifiedAttributeName(attrName);
                final boolean attrProcessed;

                // STOPSHIP: 7/6/17 The problem here is that the referenced entity attribute is encountered as Xx.xx
                // which causes failure in the getAttribute the code needs to change in order to deal with the referenced attributes
                // context also needs to track the referenced entity attributes

                if (queryType == GremlinFilterQueryType.TAG) {
                    attrProcessed          = context.hasProcessedTagAttribute(qualifiedAttributeName);
                } else {
                    attrProcessed          = context.hasProcessedEntityAttribute(qualifiedAttributeName);
                }

                // Check if the qualifiedAttribute has been processed
                if (!attrProcessed) {
                    switch (operator) {
                        case LT:
                            traversal.has(qualifiedAttributeName, ComparisonOp.LT, attrValue);
                            break;
                        case LTE:
                            traversal.has(qualifiedAttributeName, ComparisonOp.LTE, attrValue);
                            break;
                        case GT:
                            traversal.has(qualifiedAttributeName, ComparisonOp.GT, attrValue);
                            break;
                        case GTE:
                            traversal.has(qualifiedAttributeName, ComparisonOp.GTE, attrValue);
                            break;
                        case EQ:
                            traversal.has(qualifiedAttributeName, ComparisonOp.EQ, attrValue);
                            break;
                        case NEQ:
                            traversal.has(qualifiedAttributeName, ComparisonOp.NEQ, attrValue);
                            break;
                        case LIKE:
                            traversal.has(qualifiedAttributeName, PatternOp.REGEX, getLikeRegex(attrValue));
                            break;
                        case CONTAINS:
                            traversal.has(qualifiedAttributeName, PatternOp.REGEX, getContainsRegex(attrValue));
                            break;
                        case STARTS_WITH:
                            traversal.has(qualifiedAttributeName, PatternOp.PREFIX, attrValue);
                            break;
                        case ENDS_WITH:
                            traversal.has(qualifiedAttributeName, PatternOp.REGEX, getSuffixRegex(attrValue));
                            break;
                        case IN:
                            LOG.warn("{}: unsupported operator. Ignored", operator);
                            break;
                    }
                }
            } catch (AtlasBaseException e) {
                LOG.error("toGremlinTraversal(): failed for attrName=" + attrName + "; operator=" + operator + "; attrValue=" + attrValue, e);
            }
        }

        return traversal;
    }

    private String getContainsRegex(String attributeValue) {
        return ".*" + attributeValue + ".*";
    }

    private String getSuffixRegex(String attributeValue) {
        return ".*" + attributeValue;
    }

    private String getLikeRegex(String attributeValue) { return ".*" + attributeValue + ".*"; }
}
