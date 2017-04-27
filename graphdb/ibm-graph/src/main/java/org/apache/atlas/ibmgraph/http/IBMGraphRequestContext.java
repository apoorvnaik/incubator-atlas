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
package org.apache.atlas.ibmgraph.http;

/**
 * Stores the tenantId being used for each request (/thread).
 */
public class IBMGraphRequestContext {

    private String tenantId;
    private static final ThreadLocal<IBMGraphRequestContext> CURRENT_CONTEXT = new ThreadLocal<IBMGraphRequestContext>();


    private IBMGraphRequestContext() {}
    public static IBMGraphRequestContext get() {
        if (CURRENT_CONTEXT.get() == null)
        {
            createContext();
        }
        return CURRENT_CONTEXT.get();
    }

    public static IBMGraphRequestContext createContext() {
        IBMGraphRequestContext context = new IBMGraphRequestContext();
        CURRENT_CONTEXT.set(context);
        return context;
    }
    public static void clear() {
        CURRENT_CONTEXT.remove();
    }
    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        if (tenantId != null)
        {
            tenantId = tenantId.toLowerCase();
        }
        this.tenantId = tenantId;
    }

}
