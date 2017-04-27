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
package org.apache.atlas.ibmgraph.util;

import org.apache.atlas.ibmgraph.IBMGraphManagement;
import org.apache.atlas.ibmgraph.api.GraphDatabaseConfiguration;
import org.apache.atlas.ibmgraph.http.HttpRetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Http retry strategy we use creating the schema.  This customizes
 * HttpRetryStrategy to not automatically retry when the request
 * fails with a "Failed to establish backside connection" error.
 */
public class UpdateSchemaRetryStrategy extends HttpRetryStrategy {

    private static final Logger logger_ = LoggerFactory.getLogger(UpdateSchemaRetryStrategy.class);
    public static final UpdateSchemaRetryStrategy INSTANCE = new UpdateSchemaRetryStrategy();

    private UpdateSchemaRetryStrategy() {

    }

    @Override
    protected boolean isRetryInternalServerError(org.apache.http.HttpResponse response, int tryNumber) {
       String body = safelyGetResponseBody(response);
       if(body.contains(IBMGraphManagement.FAILED_TO_ESTABLISH_A_BACKSIDE_CONNECTION)) {

           //This error comes from the load balancer tier within IBM Graph.  It usually means that
           //the load balancer timed out while waiting for IBM Graph to come back with a response.
           //This means that despite the failure, IBM Graph could still be working on and completing
           //out request in the background.  We have seen that this is that case most of the time.
           //In cases like this, we want to return control back to IBMGraphManagement and let
           //it manage the retries.

           logger_.warn("Encountered Backend Connection Problem.  (try #" + tryNumber + ").  Skipping retry...");
           return false;
       }
       else {
           logRetry("Encountered Internal Server Error.", tryNumber);
           return true;
       }
    }

    @Override
    protected int getMaxRetries() {
        //Be more aggressive in retrying the schema updates, since if these fail
        //Atlas becomes unusable.  In the current environment, it is not unusual
        //for 8 or 9 retries to be required before the request succeeds.
        return GraphDatabaseConfiguration.INSTANCE.getCreateSchemaHttpMaxRetries();
    }
}