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
package org.apache.atlas.ibmgraph.api;
/**
 * Enumerates the possible states of an index within IBM Graph
 */
public enum IndexStatus {
    REGISTERED(4), //registered with all instances in the cluster
    ENABLED(5), //registered with all instances in the cluster, ready to use
    INSTALLED(3), //in the system but not registered with all instances
    DISABLED(2), //disabled, no longer in use
    NOT_PRESENT(1); //missing

    private int order_;

    private IndexStatus(int order) {
        order_ = order;
    }
    public int getOrder() {
        return order_;
    }
    public boolean isWorseThan(IndexStatus other) {
        return getOrder() < other.getOrder();
    }
    public boolean isBetterThan(IndexStatus other) {
        return getOrder() > other.getOrder();
    }

}