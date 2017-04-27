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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.atlas.ibmgraph.IBMGraphEdge;
import org.apache.atlas.ibmgraph.IBMGraphVertex;
import org.apache.atlas.ibmgraph.api.json.update.ElementChanges;
import org.apache.atlas.ibmgraph.api.json.update.ElementType;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;

/**
 * Stores the updated state of a vertex within a transaction
 *
 */
public class DefaultUpdatedVertex extends DefaultUpdatedGraphElement<IBMGraphVertex> implements UpdatedVertex {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultUpdatedVertex.class);

    //We only store the incoming/outgoing edges that were added during the current transaction.
    //There is a filtering mechanism to remove edges that have been deleted.
    //See IBMGraphGraph.wrapEdges() for details.
    private Set<IBMGraphEdge> incomingEdgesAdded_ = new HashSet<>();

    private Set<IBMGraphEdge> outgoingEdgesAdded_ = new HashSet<>();

    public DefaultUpdatedVertex(IBMGraphTransaction tx, IBMGraphVertex element) {
        super(tx, element);
    }

    @Override
    public void addIncomingEdge(IBMGraphEdge edge) {
        incomingEdgesAdded_.add(edge);

    }

    @Override
    public void removeIncomingEdge(IBMGraphEdge edge) {
        incomingEdgesAdded_.remove(edge);
    }

    @Override
    public void addOutgoingEdge(IBMGraphEdge edge) {
        outgoingEdgesAdded_.add(edge);

    }

    @Override
    public void removeOutgoingEdge(IBMGraphEdge edge) {
        outgoingEdgesAdded_.remove(edge);
    }



    @Override
    public Collection<IBMGraphEdge> getIncomingEdgesAdded() {
        return Collections.unmodifiableCollection(incomingEdgesAdded_);
    }

    @Override
    public Collection<IBMGraphEdge> getOutgoingEdgesAdded() {
        return Collections.unmodifiableCollection(outgoingEdgesAdded_);
    }


    @Override
    public Set<IBMGraphEdge> getKnownIncomingEdges() {

        //Here, we combine the incoming edges in the graph with the ones that have been added during the
        //transaction, and filter out the ones that have been deleted.

        Set<IBMGraphEdge> combined = Sets.union(wrappedElement_.getKnownIncomingEdgesInGraph(), incomingEdgesAdded_);

        return Sets.filter(combined, new Predicate<IBMGraphEdge>() {

            @Override
            public boolean apply(IBMGraphEdge input) {
                return input.exists();
            }
        });
    }

    @Override
    public Set<IBMGraphEdge> getAllOutgoingEdges() {

        //Here, we combine the outgoing edges in the graph with the ones that have been added during the
        //transaction, and filter out the ones that have been deleted.

        Set<IBMGraphEdge> combined = Sets.union(wrappedElement_.getAllOutgoingEdgesInGraph(), outgoingEdgesAdded_);
        return Sets.filter(combined, new Predicate<IBMGraphEdge>() {

            @Override
            public boolean apply(IBMGraphEdge input) {
                return input.exists();
            }
        });
    }

    @Override
    public void pushChangesIntoElement() {
        synchronized(wrappedElement_) {
            super.pushChangesIntoElement();
            wrappedElement_.applyEdgeChangesFromTransaction(getOutgoingEdgesAdded(), getIncomingEdgesAdded());
        }
    }

    @Override
    public void delete() {

        super.delete();

        for(IBMGraphEdge added : getKnownIncomingEdges()) {
            UpdatedEdge updatedEdge = tx_.getUpdatedEdge(added);
            if(!added.isDeleted()) {
                LOG.debug("Marking edge {} as removed", added);
                updatedEdge.delete(wrappedElement_);
            }

        }

        //If the outgoing edges have not been loaded, they will be marked as having been
        //deleted as part of the logic for wrapping edges we get from IBM Graph.
        //see IBMGraphEdge.setFieldsFromLoadedInfo.  There's no need to load them just to mark them
        //as deleted.
        if(wrappedElement_.isOutgoingEdgesLoaded()) {
            for(AtlasEdge<IBMGraphVertex,IBMGraphEdge> outgoingEdge : getAllOutgoingEdges()) {
                UpdatedEdge updatedEdge = tx_.getUpdatedEdge(outgoingEdge.getE());
                if(!outgoingEdge.getE().isDeleted()) {
                    LOG.debug("Marking edge {} as removed", updatedEdge);
                    updatedEdge.delete(wrappedElement_);
                }

            }
        }
        incomingEdgesAdded_.clear();
        outgoingEdgesAdded_.clear();
    }

    @Override
    protected ElementChanges createElementChangesInstance() {
        return new ElementChanges(ElementType.vertex);
    }

}
