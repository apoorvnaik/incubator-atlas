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
package org.apache.atlas.web.service;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.Atlas;
import org.apache.atlas.AtlasException;
import org.apache.atlas.web.TestUtils;
import org.apache.atlas.web.resources.AdminJerseyResourceIT;
import org.apache.atlas.web.resources.EntityJerseyResourceIT;
import org.apache.atlas.web.resources.MetadataDiscoveryJerseyResourceIT;
import org.apache.atlas.web.resources.TypesJerseyResourceIT;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.alias.JavaKeyStoreProvider;
import org.testng.Assert;
import org.testng.TestListenerAdapter;
import org.testng.TestNG;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.ws.rs.core.UriBuilder;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.KeyStore;

import static org.apache.atlas.security.SecurityProperties.*;

/**
 * Secure Test class for jersey resources.
 */
public class SecureEmbeddedServerTestBase {


    public static final int ATLAS_DEFAULT_HTTPS_PORT = 21443;
    private SecureEmbeddedServer secureEmbeddedServer;
    protected String providerUrl;
    private Path jksPath;
    protected WebResource service;
    private int securePort;


    @BeforeClass
    public void setupSecurePort() throws AtlasException {
        org.apache.commons.configuration.Configuration configuration = ApplicationProperties.get();
        securePort = configuration.getInt(Atlas.ATLAS_SERVER_HTTPS_PORT, ATLAS_DEFAULT_HTTPS_PORT);
    }

    @BeforeMethod
    public void setup() throws Exception {
        jksPath = new Path(Files.createTempDirectory("tempproviders").toString(), "test.jks");
        providerUrl = JavaKeyStoreProvider.SCHEME_NAME + "://file/" + jksPath.toUri();

        String baseUrl = String.format("https://localhost:%d/", securePort);

        DefaultClientConfig config = new DefaultClientConfig();
        Client client = Client.create(config);
        client.resource(UriBuilder.fromUri(baseUrl).build());

        service = client.resource(UriBuilder.fromUri(baseUrl).build());
    }

    @Test
    public void testNoConfiguredCredentialProvider() throws Exception {
        String originalConf = null;
        try {
            originalConf = System.getProperty("atlas.conf");
            System.clearProperty("atlas.conf");
            ApplicationProperties.forceReload();
            secureEmbeddedServer = new SecureEmbeddedServer(securePort, TestUtils.getWarPath());
            secureEmbeddedServer.server.start();

            Assert.fail("Should have thrown an exception");
        } catch (IOException e) {
            Assert.assertEquals(e.getMessage(),
                    "No credential provider path configured for storage of certificate store passwords");
        } finally {
            if (secureEmbeddedServer != null) {
                secureEmbeddedServer.server.stop();
            }

            if (originalConf == null) {
                System.clearProperty("atlas.conf");
            } else {
                System.setProperty("atlas.conf", originalConf);
            }
        }
    }

    @Test
    public void testMissingEntriesInCredentialProvider() throws Exception {
        // setup the configuration
        final PropertiesConfiguration configuration = new PropertiesConfiguration();
        configuration.setProperty(CERT_STORES_CREDENTIAL_PROVIDER_PATH, providerUrl);

        try {
            secureEmbeddedServer = new SecureEmbeddedServer(securePort, TestUtils.getWarPath()) {
                @Override
                protected PropertiesConfiguration getConfiguration() {
                    return configuration;
                }
            };
            Assert.fail("No entries should generate an exception");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().startsWith("No credential entry found for"));
        } finally {
            secureEmbeddedServer.server.stop();
        }

    }

    /**
     * Runs the existing webapp test cases, this time against the initiated secure server instance.
     * @throws Exception
     */
    @Test
    public void runOtherSuitesAgainstSecureServer() throws Exception {
        final PropertiesConfiguration configuration = new PropertiesConfiguration();
        configuration.setProperty(CERT_STORES_CREDENTIAL_PROVIDER_PATH, providerUrl);
        // setup the credential provider
        setupCredentials();

        try {
            secureEmbeddedServer = new SecureEmbeddedServer(securePort, TestUtils.getWarPath()) {
                @Override
                protected PropertiesConfiguration getConfiguration() {
                    return configuration;
                }
            };
            secureEmbeddedServer.server.start();

            TestListenerAdapter tla = new TestListenerAdapter();
            TestNG testng = new TestNG();
            testng.setTestClasses(new Class[]{AdminJerseyResourceIT.class, EntityJerseyResourceIT.class,
                    MetadataDiscoveryJerseyResourceIT.class,
                    TypesJerseyResourceIT.class});
            testng.addListener(tla);
            testng.run();

        } finally {
            secureEmbeddedServer.server.stop();
        }

    }

    protected void setupCredentials() throws Exception {
        Configuration conf = new Configuration(false);

        File file = new File(jksPath.toUri().getPath());
        file.delete();
        conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, providerUrl);

        CredentialProvider provider = CredentialProviderFactory.getProviders(conf).get(0);

        // create new aliases
        try {

            char[] storepass = {'k', 'e', 'y', 'p', 'a', 's', 's'};
            provider.createCredentialEntry(KEYSTORE_PASSWORD_KEY, storepass);

            char[] trustpass = {'k', 'e', 'y', 'p', 'a', 's', 's'};
            provider.createCredentialEntry(TRUSTSTORE_PASSWORD_KEY, trustpass);

            char[] certpass = {'k', 'e', 'y', 'p', 'a', 's', 's'};
            provider.createCredentialEntry(SERVER_CERT_PASSWORD_KEY, certpass);

            // write out so that it can be found in checks
            provider.flush();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    protected HostnameVerifier createLocalhostOnlyHostnameVerifier() {
        return new HostnameVerifier() {

            public boolean verify(String hostname, SSLSession sslSession) {
                return hostname.equals("localhost");
            }

        };
    }

    protected TrustManager[] createClientTrustManagers() throws Exception {
        KeyStore trustStore = loadAtlasKeyStore();
        String algorithm = TrustManagerFactory.getDefaultAlgorithm();
        TrustManagerFactory factory = TrustManagerFactory.getInstance(algorithm);
        factory.init(trustStore);
        return factory.getTrustManagers();
    }

    protected KeyStore loadAtlasKeyStore() throws Exception {
        KeyStore store = KeyStore.getInstance("JKS");
        File keystoreFile = new File(TestUtils.getWebAppDirectory() + "/" + DEFAULT_KEYSTORE_FILE_LOCATION);
        if(! keystoreFile.exists()) {
            throw new AtlasException("KeyStore " + keystoreFile.getAbsolutePath() + " does not exist!");
        }
        InputStream is = new FileInputStream(keystoreFile);
        try {
            store.load(is, new char[] {'k', 'e', 'y', 'p', 'a', 's', 's'});
        } finally {
            is.close();
        }
        return store;
    }
}
