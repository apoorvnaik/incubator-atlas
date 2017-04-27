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
 * The list of standard HTTP status codes
 */
public interface HttpCode {

    static final int CREATED = 201;
    static final int CONFLICT = 409;
    static final int OK = 200;
    static final int BAD_REQUEST = 400;
    static final int INTERNAL_SERVER_ERROR = 500;
    static final int NOT_FOUND = 404;
    static final int ACCEPTED = 202;
    static final int TIME_OUT = 504;
    static final int BAD_GATEWAY = 502;
    static final int FORBIDDEN = 403;
    static final int AUTHENTICATION_ERROR = 401;
}
