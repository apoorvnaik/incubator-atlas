///**
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// * <p>
// * http://www.apache.org/licenses/LICENSE-2.0
// * <p>
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.apache.atlas.repository.graphdb.titan0;
//
//import org.apache.atlas.ApplicationProperties;
//import org.apache.atlas.AtlasException;
//import org.apache.commons.configuration.Configuration;
//import org.apache.commons.lang3.RandomStringUtils;
//
//import java.io.File;
//
//public class GraphSandbox {
//    public static void create() {
//        Configuration configuration;
//        try {
//            configuration = ApplicationProperties.get();
//            String currentStorageDir = configuration.getString("atlas.graph.storage.directory");
//            String currentIndexDir = configuration.getString("atlas.graph.index.search.directory");
//
//            // Append a suffix to isolate the database for each instance
//            configuration.setProperty("atlas.graph.storage.directory", currentStorageDir + File.separator + RandomStringUtils.randomAlphabetic(5));
//            configuration.setProperty("atlas.graph.index.search.directory", currentIndexDir + File.separator + RandomStringUtils.randomAlphabetic(5));
//        } catch (AtlasException ignored) {}
//    }
//}
