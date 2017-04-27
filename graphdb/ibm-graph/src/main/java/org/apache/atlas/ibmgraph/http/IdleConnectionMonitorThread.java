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

import java.util.concurrent.TimeUnit;

import org.apache.atlas.ibmgraph.api.GraphDatabaseConfiguration;
import org.apache.http.conn.HttpClientConnectionManager;

/**
 * Thread that maintains the http connection pool by periodically closing expired
 * and idle connections.
 *
 * See http://hc.apache.org/httpcomponents-client-ga/tutorial/html/connmgmt.html#d5e659
 */
public class IdleConnectionMonitorThread extends Thread {

    private final HttpClientConnectionManager connMgr;
    private volatile boolean shutdown;

    public IdleConnectionMonitorThread(HttpClientConnectionManager connMgr) {
        super();
        this.connMgr = connMgr;
        this.setName("IBM Graph Idle Connection Monitor");
    }

    @Override
    public void run() {
        try {
            while (!shutdown) {
                synchronized (this) {
                    wait(5000);
                    // Close expired connections
                    connMgr.closeExpiredConnections();

                    // Close connections that have been idle too long.
                    int idleTimeout = GraphDatabaseConfiguration.INSTANCE.getHttpIdleTimeoutMs();
                    if(idleTimeout > 0) {
                        connMgr.closeIdleConnections(idleTimeout, TimeUnit.MILLISECONDS);
                    }
                }
            }
        } catch (InterruptedException ex) {
            // terminate
        }
    }

    public void shutdown() {
        shutdown = true;
        synchronized (this) {
            notifyAll();
        }
    }

}