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

import org.apache.atlas.ibmgraph.IBMGraphVertex;

/**
 * Represents the updated state of an edge within a transaction
 *
 */
public interface UpdatedEdge extends UpdatedGraphElement, ReadableUpdatedEdge {

    /**
     * Deletes the edge, skipping any updated that would otherwise be applied to
     * the specified vertex. If no vertices should be skipped, null should be
     * passed, or, equivalently, the no-argument delete() method can be called.
     *
     * This is to avoid updating the same vertex twice. This is used when a
     * vertex is deleted and we are propagating that deletion to the
     * incoming/outgoing edges associated with that vertex. It is used to avoid
     * updating the vertex that triggered the delete (and an infinite loop).
     */
    void delete(IBMGraphVertex vertexToSkip);
}
