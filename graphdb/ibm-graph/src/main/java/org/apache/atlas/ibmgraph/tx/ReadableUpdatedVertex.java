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
import java.util.Set;

import org.apache.atlas.ibmgraph.IBMGraphEdge;

/**
 * Represents the updated state of a vertex within a transaction
 *
 */
public interface ReadableUpdatedVertex extends ReadableUpdatedGraphElement {


    /**
     * Gets the incoming edges that have been added during this transaction.  Only
     * relevent for vertices.
     *
     * @return
     */
    Collection<IBMGraphEdge> getIncomingEdgesAdded();

    /**
     * Gets the outgoing edges that have been added during this transaction.  Only
     * relevent for vertices.
     *
     * @return
     */
    Collection<IBMGraphEdge> getOutgoingEdgesAdded();


    /**
     * Returns the list of all outgoing edges for this vertex, taking into account
     * changes that have been made within the context of the current transaction.
     *
     * @return
     */
    Set<IBMGraphEdge> getAllOutgoingEdges();

    /**
     * Returns the list of all incoming edges for this vertex that have been
     * retrieved from the graph, taking into account changes that have been made
     * within the context of the current transaction.
     *
     * @return
     */

    Set<IBMGraphEdge> getKnownIncomingEdges();

}
