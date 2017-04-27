/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.web;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.net.URISyntaxException;
import java.net.URL;

public class TestUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);

    public static String random(){
        return RandomStringUtils.randomAlphanumeric(10);
    }

    public static void writeConfiguration(PropertiesConfiguration configuration, String fileName) throws Exception {
        LOG.debug("Storing configuration in file {}", fileName);
        File file = new File(fileName);
        File parentFile = file.getParentFile();
        if (!parentFile.exists() && !parentFile.mkdirs()) {
            throw new Exception("Failed to create dir " + parentFile.getAbsolutePath());
        }
        file.createNewFile();
        configuration.save(new FileWriter(file));
    }

    public static String getTempDirectory() {
        return getProjectBaseDir() + "/webapp/target/" + random();
    }

    public static String getWarPath() {
        return getProjectBaseDir() + String.format("/webapp/target/atlas-webapp-%s",
                System.getProperty("project.version"));
    }

    public static String getTargetDirectory() {
        return getProjectBaseDir() + "/webapp/target";
    }

    public static String getWebAppDirectory() {
        return getProjectBaseDir() + "/webapp";
    }

    public static String getProjectBaseDir() {
        String projectBaseDir = System.getProperty("projectBaseDir");
        if (projectBaseDir == null) {
            // In the case the test is run directly from Eclipse
            projectBaseDir = locateProjectBaseDirFromClassPath();
        }
        return projectBaseDir;
    }

    private static String locateProjectBaseDirFromClassPath() {
        String projectBaseDir = null;
        Class<?> class_ = TestUtils.class;
        String classResource = String.format("/%s.class", class_.getName().replaceAll("\\.", "/"));
        URL classUrl = class_.getResource(classResource);
        if (classUrl != null) {
            try {
                File classFile = new File(classUrl.toURI());
                File webappProjectDir = null;
                File current = classFile.getParentFile();
                while (webappProjectDir == null && current != null) {
                    File potentialPomFile = new File(current, "pom.xml");
                    if (potentialPomFile.exists()) {
                        webappProjectDir = current;
                    } else {
                        current = current.getParentFile();
                    }
                }
                if (webappProjectDir != null) {
                    projectBaseDir = webappProjectDir.getParentFile().getAbsolutePath();
                }
                System.out.println();
            } catch (URISyntaxException e) {
                LOG.warn("Cannot locate class file: " + e.getMessage());
            }
        }
        return projectBaseDir;
    }

}
