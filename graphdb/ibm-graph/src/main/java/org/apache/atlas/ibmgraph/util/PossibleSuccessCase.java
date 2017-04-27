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

import org.apache.atlas.ibmgraph.exception.HttpException;

public class PossibleSuccessCase {
    private int httpCode;
    private String messageToCheck;
    public PossibleSuccessCase(int httpCode, String messageToCheck) {
        super();
        this.httpCode = httpCode;
        this.messageToCheck = messageToCheck;
    }
    public int getHttpCode() {
        return httpCode;
    }
    public String getMessageToCheck() {
        return messageToCheck;
    }

    public boolean matches(HttpException e) {
        return e.getStatusCode() == getHttpCode() && GraphDBUtil.responseContains(e, getMessageToCheck());
    }

}