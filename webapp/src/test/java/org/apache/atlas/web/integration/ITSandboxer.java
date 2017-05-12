/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.web.integration;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.graph.GraphSandboxUtil;
import org.apache.atlas.kafka.KafkaNotification;
import org.apache.atlas.kafka.KafkaNotificationProvider;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.TestListenerAdapter;

import java.io.File;
import java.io.IOException;

public class ITSandboxer extends TestListenerAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(ITSandboxer.class);
    private int zookeeperPort;
    private int kafkaPort;
    private int jettyPort;

    @Override
    public void onStart(ITestContext context) {
        LOG.info("Attempting sandbox of Graph, Kafka and Zookeeper");

        GraphSandboxUtil.create();

        setApplicationHome();
        createSandbox();
    }

    @Override
    public void onFinish(ITestContext context) {
        AtlasGraphProvider.cleanup();
    }

    private void setApplicationHome() {
        if (System.getProperty("atlas.home") == null) {
            System.setProperty("atlas.home", "target");
        }
        if (System.getProperty("atlas.data") == null) {
            System.setProperty("atlas.data", "target/data");
        }
        if (System.getProperty("atlas.log.dir") == null) {
            System.setProperty("atlas.log.dir", "target/logs");
        }
    }

    private void createSandbox() {
        try {
            Configuration configuration = ApplicationProperties.get();

            File testFile = new File(getClass().getClassLoader().getResource("").getPath() + "atlas-test-config.txt");
            if (!testFile.exists()) testFile.createNewFile();

            PropertiesConfiguration testConfiguration = new PropertiesConfiguration(testFile.getAbsolutePath());

            zookeeperPort = testConfiguration.getInt("zkPort", 19000) + 10;
            kafkaPort = testConfiguration.getInt("kafkaPort", 20000) + 10;
            jettyPort = testConfiguration.getInt("jettyPort", 30000) + 10;

            testConfiguration.setProperty("zkPort", zookeeperPort);
            testConfiguration.setProperty("kafkaPort", kafkaPort);
            testConfiguration.setProperty("jettyPort", jettyPort);

            testConfiguration.save();

            LOG.info("Changing zookeeper port to {}", zookeeperPort);
            LOG.info("Changing kafka port to {}", kafkaPort);
            LOG.info("Changing jetty port to {}", jettyPort);

            configuration.setProperty("atlas.kafka.zookeeper.connect", "localhost:" + zookeeperPort);
            configuration.setProperty("atlas.kafka.bootstrap.servers", "localhost:" + kafkaPort);
            if (StringUtils.isEmpty(System.getProperty("atlas.data"))) {
                // TODO: Validate if this works
                System.setProperty("atlas.data", getClass().getClassLoader().getResource("").getPath());
            }
            String kafkaData = System.getProperty("atlas.data") +
                    File.pathSeparator + System.currentTimeMillis() +
                    File.pathSeparator + "kafka";

            LOG.info("Sandboxed Kafka data to {}", kafkaData);

            configuration.setProperty("atlas.kafka.data", kafkaData);
        } catch (ConfigurationException | IOException | AtlasException e) {
            LOG.error("Test config not loaded");
        }

    }

    public static class SandboxedNotificationModule extends AbstractModule {
        @Override
        protected void configure() {
            bind(NotificationInterface.class).to(KafkaNotification.class).asEagerSingleton();
            bind(KafkaNotification.class).toProvider(TestNotificationProvider.class).in(Singleton.class);
        }
    }

    static class TestNotificationProvider extends KafkaNotificationProvider {
        private static KafkaNotification kafkaNotification;

        @Override
        public KafkaNotification get() {
            if (null == kafkaNotification) {
                kafkaNotification = super.get();
            }
            return kafkaNotification;
        }
    }
}
