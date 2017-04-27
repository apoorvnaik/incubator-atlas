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
import org.apache.atlas.ibmgraph.util.PropertyIndex;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;

/**
 * Represents a transaction within IBM Graph
 *
 */
public interface GraphTransaction {

    /**
     * Rolls back the transaction
     */
    void rollback();

    /**
     * Forces the pending changes in the transaction to be forced into the graph
     * so that we can execute some action that requires them to be there, such
     * as executing a non-trivial query.  Note that doing this outside the context
     * of a commit or rollback is inherently unsafe and violates the transaction semantics.
     * This effectively is forcing the transaction to be committed prematurely.  While we have
     * made an effort to avoid doing this wherever possible, there are still some situations
     * where we can't answer a graph query correctly without doing this.
     *
     */
    void applyPendingChanges();

    /**
     * Commits the changes within the transaction
     */
    void commit();

    /**
     * Adds a vertex within the transaction
     * @return
     */
    AtlasVertex<IBMGraphVertex, IBMGraphEdge> addVertex();

    /**
     * Adds an edge within the transaction
     * @param outVertex
     * @param inVertex
     * @param label
     * @return
     */
    AtlasEdge<IBMGraphVertex, IBMGraphEdge> addEdge(IBMGraphVertex outVertex, IBMGraphVertex inVertex, String label);

    /**
     * Gets the index of property changes made within the transaction
     * @return
     */
    PropertyIndex getPropertyChangeIndex();

    /**
     * Gets a read-only view of the updated vertex that combines the
     * changes that have been committed in the graph with the changes
     * that have been applied in this transaction.  If there are
     * no changes, null will be returned.
     *
     * @return ReadableUpdatedVertex with the changes or null if there are no
     *    changes in the current transaction.
     */
    ReadableUpdatedVertex getReadOnlyUpdatedVertex(IBMGraphVertex element);

    /**
     * Gets a read-only view of the updated edge that combines the
     * changes that have been committed in the graph with the changes
     * that have been applied in this transaction.  If there are
     * no changes, null will be returned.
     *
     * @return ReadableUpdatedEdge with the changes or null if there are no
     *    changes in the current transaction.
     */
    ReadableUpdatedEdge getReadOnlyUpdatedEdge(IBMGraphEdge element);

    /**
     * Gets the state of the given vertex within this transaction.  Any changes
     * made will be persisted as part of the transaction.
     *
     * @param element
     * @return
     */
    UpdatedVertex getUpdatedVertex(IBMGraphVertex vertex);

    /**
     * Gets the state of the given edge within this transaction.  Any changes
     * made will be persisted as part of the transaction.
     *
     * @param element
     * @return
     */
    UpdatedEdge getUpdatedEdge(IBMGraphEdge edge);

}