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

import org.apache.atlas.ibmgraph.IBMGraphEdge;
import org.apache.atlas.ibmgraph.IBMGraphVertex;
import org.apache.atlas.ibmgraph.api.json.update.ElementChanges;
import org.apache.atlas.ibmgraph.api.json.update.ElementType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores the updated state of an edge within a transaction
 *
 */
public class DefaultUpdatedEdge extends DefaultUpdatedGraphElement<IBMGraphEdge> implements UpdatedEdge {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultUpdatedEdge.class);
    public DefaultUpdatedEdge(IBMGraphTransaction tx, IBMGraphEdge element) {
        super(tx, element);
    }

    @Override
    public void delete() {
        delete(null);
    }

    @Override
    public void delete(IBMGraphVertex vertexToSkip) {

        LOG.debug("Marking edge {} as removed", this);

        super.delete();
        if(! (wrappedElement_.isProxy() || wrappedElement_.isResolvingProxy())) {
            IBMGraphVertex inVertex = wrappedElement_.getInVertex().getV();
            if(! inVertex.equals(vertexToSkip)) {
                tx_.getUpdatedVertex(inVertex).removeIncomingEdge(wrappedElement_);
            }

            IBMGraphVertex outVertex = wrappedElement_.getOutVertex().getV();
            if(! outVertex.equals(vertexToSkip)) {
                tx_.getUpdatedVertex(outVertex).removeOutgoingEdge(wrappedElement_);
            }
        }
    }

    @Override
    protected ElementChanges createElementChangesInstance() {
        return new ElementChanges(ElementType.edge);
    }
}
