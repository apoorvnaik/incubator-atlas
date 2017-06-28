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
import org.apache.atlas.model.discovery.SearchParameters.Operator;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v1.AtlasGraphUtilsV1;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.apache.atlas.discovery.SearchPipeline.IndexResultType;
import static org.apache.atlas.discovery.SearchPipeline.PipelineContext;
import static org.apache.atlas.discovery.SearchPipeline.PipelineStep;
import static org.apache.atlas.repository.graphdb.AtlasGraphQuery.ComparisionOperator;
import static org.apache.atlas.repository.graphdb.AtlasGraphQuery.MatchingOperator;

@Component
public class GremlinStep implements PipelineStep {
    public static final String TAG_VERTEX_CACHE_NAME = "TAG_VERTEX";
    private static final Logger LOG      = LoggerFactory.getLogger(GremlinStep.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("GremlinSearchStep");

    private final AtlasGraph        graph;
    private final AtlasTypeRegistry typeRegistry;

    enum GremlinFilterQueryType { TAG, ENTITY }

    @Inject
    public GremlinStep(AtlasGraph graph, AtlasTypeRegistry typeRegistry) {
        this.graph        = graph;
        this.typeRegistry = typeRegistry;
    }

    @Override
    public void execute(PipelineContext context) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> execute({})", context);
        }

        AtlasPerfTracer perf = null;

        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GremlinSearchStep.execute(" + context+  ")");
        }

        SearchParameters searchParameters = context.getSearchParameters();

        if (context.hasIndexResults()) {
            // We have some results from the indexed step, let's proceed accordingly
            if (context.getIndexResultType() == IndexResultType.TAG) {
                // Index search was done on tag and filters
                if (context.isTagProcessingComplete()) {
                    LOG.debug("Index has completely processed tag, further TAG filtering not needed");
                    Iterator<AtlasIndexQuery.Result> tagVertexIterator = context.getIndexResultsIterator();

                    // No tag association found
                    if (!tagVertexIterator.hasNext()) {
                        return;
                    }

                    Set<String> taggedVertexGUIDs = new HashSet<>();

                    // Tag associations found
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
                    if (taggedVertexGUIDs.isEmpty()) {
                        return;
                    }

                    processEntity(taggedVertexGUIDs, context);
                } else {
                    processTagAndEntity(context);
                }
            }

            if (context.getIndexResultType() == IndexResultType.ENTITY) {
                // Index step processed entity and it's filters, now either the tag wasn't processed at all or
                // tag is absent in request
                Set<String> entityIDs = getVertexIDs(context.getIndexResultsIterator());
                if (StringUtils.isNotEmpty(searchParameters.getClassification())) {
                    // TODO: Determine the outgoing edges from the entity vertex and then apply tag filter on those
//                    Iterable<AtlasEdge> edges = graph.query()
//                            .in(Constants.GUID_PROPERTY_KEY, entityIDs)
//                            .has(Constants.TRAIT_NAMES_PROPERTY_KEY, searchParameters.getClassification())
//                            .edges();
//
//                    // Get all
//                    String tagVertexGUID = null;
//                    for (AtlasEdge edge : edges) {
//                        String expectedEdgeLabel = GraphHelper.getTraitLabel(searchParameters.getTypeName(), searchParameters.getClassification());
//                        if (StringUtils.equals(expectedEdgeLabel, edge.getLabel())) {
//                            tagVertexGUID = AtlasGraphUtilsV1.getIdFromVertex(edge.getInVertex());
//                            break;
//                        }
//                    }
//                    AtlasGraphQuery tagFilterQuery = graph.query().has(Constants.GUID_PROPERTY_KEY, tagVertexGUID);
//                    if (null != searchParameters.getTagFilters()) {
//                        toGremlinFilterQuery(GremlinFilterQueryType.TAG,
//                                searchParameters.getClassification(), searchParameters.getTagFilters(),
//                                tagFilterQuery, context);
//                    }

                } else {
                    // Only Entity needs to be processed by Gremlin
                    processEntity(entityIDs, context);
                }
            }
        } else {
            // No index results, need full processing in Gremlin
            if (StringUtils.isNotEmpty(searchParameters.getClassification())) {
                // Process tag and filters first, then entity filters
                processTagAndEntity(context);
            } else {
                processEntity(Collections.<String>emptySet(), context);
            }
        }

        AtlasPerfTracer.log(perf);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== execute({})", context);
        }
    }

    private void processEntity(Set<String> entityGUIDs, PipelineContext context) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> ProcessEntity({})", entityGUIDs);
        }

        SearchParameters searchParameters = context.getSearchParameters();

        if (StringUtils.isNotEmpty(searchParameters.getTypeName())) {
            // No Tag related processing, process everything related to the entity
            AtlasEntityType typeByName = typeRegistry.getEntityTypeByName(searchParameters.getTypeName());
            if (typeByName == null) {
                // If no such type exists then doesn't make any sense to process anything
                LOG.warn("Invalid TypeName:{} specified", searchParameters.getTypeName());
                return;
            }

            // Entities need to match the derived types too
            AtlasGraphQuery entityFilterQuery = context.getGraphQuery("ENTITY_FILTER");
            if (null == entityFilterQuery) {
                // Don't add TypeName filter as it messes the index
//                entityFilterQuery = graph.query().in(Constants.TYPE_NAME_PROPERTY_KEY, typeByName.getTypeAndAllSubTypes());
                entityFilterQuery = graph.query();

                if (searchParameters.getEntityFilters() != null) {
                    toGremlinFilterQuery(GremlinFilterQueryType.ENTITY, searchParameters.getTypeName(), searchParameters.getEntityFilters(), entityFilterQuery, context);
                }
                context.cacheGraphQuery("ENTITY_FILTER", entityFilterQuery);
            }

            // Now get all vertices
            Iterator<AtlasVertex> vertexIterator;
            if (CollectionUtils.isEmpty(entityGUIDs)) {
                vertexIterator = entityFilterQuery.vertices(context.getCurrentOffset(), context.getMaxLimit()).iterator();
            } else {
                AtlasGraphQuery guidQuery = graph.query().in(Constants.GUID_PROPERTY_KEY, entityGUIDs);
                if (null != entityFilterQuery) {
                    guidQuery.addConditionsFrom(entityFilterQuery);
                }

                if (searchParameters.getExcludeDeletedEntities()) {
                    guidQuery.has(Constants.STATE_PROPERTY_KEY, "ACTIVE");
                }

                vertexIterator = guidQuery
                        .vertices(context.getCurrentOffset(), context.getMaxLimit()).iterator();
            }

            vertexIterator = filterOnTypeName(vertexIterator, typeByName.getTypeAndAllSubTypes());

            if (LOG.isDebugEnabled()) {
                LOG.debug("Current running offset {}", context.getCurrentOffset());
            }

            context.setGremlinResultIterator(vertexIterator);
        } else {
            if (CollectionUtils.isEmpty(entityGUIDs))  return;

            AtlasGraphQuery guidQuery = graph.query().in(Constants.GUID_PROPERTY_KEY, entityGUIDs);
            if (searchParameters.getExcludeDeletedEntities()) {
                guidQuery.has(Constants.STATE_PROPERTY_KEY, "ACTIVE");
            }
            Iterable<AtlasVertex> vertices = guidQuery.vertices(context.getCurrentOffset(), context.getMaxLimit());
            context.setGremlinResultIterator(vertices.iterator());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== ProcessEntity({})", entityGUIDs);
        }
    }

    private Iterator<AtlasVertex> filterOnTypeName(Iterator<AtlasVertex> vertexIterator, Set<String> typeAndAllSubTypes) {
        if (vertexIterator == null) {
            return null;
        }

        List<AtlasVertex> matchingVertices = new ArrayList<>();
        while(vertexIterator.hasNext()) {
            AtlasVertex vertex = vertexIterator.next();
            String typeName = AtlasGraphUtilsV1.getProperty(vertex, Constants.ENTITY_TYPE_PROPERTY_KEY, String.class);
            if (CollectionUtils.isNotEmpty(typeAndAllSubTypes) && typeAndAllSubTypes.contains(typeName))
                matchingVertices.add(vertex);
        }

        return matchingVertices.iterator();
    }

    private void processTagAndEntity(PipelineContext context) {
        SearchParameters searchParameters = context.getSearchParameters();

        AtlasGraphQuery tagVertexQuery = context.getGraphQuery(TAG_VERTEX_CACHE_NAME);
        if (tagVertexQuery == null) {
            try {
                typeRegistry.getType(searchParameters.getClassification());
            } catch (AtlasBaseException e) {
                LOG.warn("Tag {} doesn't exist.", searchParameters.getClassification());
                return;
            }

            tagVertexQuery = graph
                    .query()
                    .has(Constants.TYPE_NAME_PROPERTY_KEY, searchParameters.getClassification());

            // Do tag filtering first as it'll return a smaller subset of vertices
            if (searchParameters.getTagFilters() != null) {
                toGremlinFilterQuery(GremlinFilterQueryType.TAG,
                        searchParameters.getClassification(), searchParameters.getTagFilters(),
                        tagVertexQuery, context);
            }
            context.cacheGraphQuery(TAG_VERTEX_CACHE_NAME, tagVertexQuery);
        }

        Set<String> taggedVertexGuids = new HashSet<>();
        // Now get all vertices after adjusting offset for each iteration
        LOG.debug("Firing TAG query");

        Iterator<AtlasVertex> tagVertexIterator = tagVertexQuery
                .vertices(context.getCurrentOffset(), context.getMaxLimit())
                .iterator();

        // No tag association found
        if (!tagVertexIterator.hasNext()) {
            return;
        }

        // Tag associations found
        while (tagVertexIterator.hasNext()) {
            // Find out which Vertex has this outgoing edge
            Iterable<AtlasEdge> edges = tagVertexIterator.next().getEdges(AtlasEdgeDirection.IN);
            for (AtlasEdge edge : edges) {
                String guid = AtlasGraphUtilsV1.getIdFromVertex(edge.getOutVertex());
                taggedVertexGuids.add(guid);
            }
        }

        // No entities are tagged  (actually this check is already done)
        if (taggedVertexGuids.isEmpty()) {
            return;
        }

        processEntity(taggedVertexGuids, context);
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

    private AtlasGraphQuery toGremlinFilterQuery(GremlinFilterQueryType queryType, String typeName, FilterCriteria criteria,
                                                 AtlasGraphQuery query, PipelineContext context) {
        if (criteria.getCondition() != null) {
            if (criteria.getCondition() == Condition.AND) {
                for (FilterCriteria filterCriteria : criteria.getCriterion()) {
                    AtlasGraphQuery nestedQuery = toGremlinFilterQuery(queryType, typeName, filterCriteria, graph.query(), context);
                    query.addConditionsFrom(nestedQuery);
                }
            } else {
                List<AtlasGraphQuery> orConditions = new LinkedList<>();

                for (FilterCriteria filterCriteria : criteria.getCriterion()) {
                    AtlasGraphQuery nestedQuery = toGremlinFilterQuery(queryType, typeName, filterCriteria, graph.query(), context);
                    // FIXME: Something might not be right here as the queries are getting overwritten sometimes
                    orConditions.add(graph.query().createChildQuery().addConditionsFrom(nestedQuery));
                }

                if (!orConditions.isEmpty()) {
                    query.or(orConditions);
                }
            }
        } else {
            String   attrName  = criteria.getAttributeName();
            String   attrValue = criteria.getAttributeValue();
            Operator operator  = criteria.getOperator();

            try {
                // If attribute belongs to supertype then adjust the name accordingly
                final String  qualifiedAttributeName;
                final boolean attrProcessed;

                if (queryType == GremlinFilterQueryType.TAG) {
                    AtlasClassificationType tagType = typeRegistry.getClassificationTypeByName(typeName);

                    qualifiedAttributeName = tagType.getQualifiedAttributeName(attrName);
                    attrProcessed          = context.hasProcessedTagAttribute(qualifiedAttributeName);
                } else {
                    AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);

                    qualifiedAttributeName = entityType.getQualifiedAttributeName(attrName);
                    attrProcessed          = context.hasProcessedEntityAttribute(qualifiedAttributeName);
                }

                // Check if the qualifiedAttribute has been processed
                if (!attrProcessed) {
                    switch (operator) {
                        case LT:
                            query.has(qualifiedAttributeName, ComparisionOperator.LESS_THAN, attrValue);
                            break;
                        case LTE:
                            query.has(qualifiedAttributeName, ComparisionOperator.LESS_THAN_EQUAL, attrValue);
                            break;
                        case GT:
                            query.has(qualifiedAttributeName, ComparisionOperator.GREATER_THAN, attrValue);
                            break;
                        case GTE:
                            query.has(qualifiedAttributeName, ComparisionOperator.GREATER_THAN_EQUAL, attrValue);
                            break;
                        case EQ:
                            query.has(qualifiedAttributeName, ComparisionOperator.EQUAL, attrValue);
                            break;
                        case NEQ:
                            query.has(qualifiedAttributeName, ComparisionOperator.NOT_EQUAL, attrValue);
                            break;
                        case LIKE:
                            // TODO: Maybe we need to validate pattern
                            query.has(qualifiedAttributeName, MatchingOperator.REGEX, getLikeRegex(attrValue));
                            break;
                        case CONTAINS:
                            query.has(qualifiedAttributeName, MatchingOperator.CONTAINS, attrValue);
                            break;
                        case STARTS_WITH:
                            query.has(qualifiedAttributeName, MatchingOperator.PREFIX, attrValue);
                            break;
                        case ENDS_WITH:
                            query.has(qualifiedAttributeName, MatchingOperator.REGEX, getSuffixRegex(attrValue));
                        case IN:
                            LOG.warn("{}: unsupported operator. Ignored", operator);
                            break;
                    }
                }
            } catch (AtlasBaseException e) {
                LOG.error("toGremlinFilterQuery(): failed for attrName=" + attrName + "; operator=" + operator + "; attrValue=" + attrValue, e);
            }
        }

        return query;
    }

    private String getSuffixRegex(String attributeValue) {
        return ".*" + attributeValue;
    }

    private String getLikeRegex(String attributeValue) { return ".*" + attributeValue + ".*"; }
}
