/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.rest;

import static org.apache.brooklyn.KarafTestUtils.defaultOptionsWith;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.editConfigurationFilePut;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.features;

import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.inject.Inject;

import org.apache.brooklyn.KarafTestUtils;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.entity.factory.ApplicationBuilder;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.http.HttpAsserts;
import org.apache.brooklyn.util.http.HttpTool;
import org.apache.brooklyn.util.http.HttpToolResponse;
import org.apache.brooklyn.util.net.Urls;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.karaf.features.BootFinished;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;
import org.ops4j.pax.exam.util.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;


/**
 * Tests the apache-brooklyn karaf runtime assembly.
 *
 * Keeping it a non-integration test so we have at least a basic OSGi sanity check. (takes 14 sec)
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerClass.class)
public class BrooklynRestApiLauncherTest {
    private static final Logger LOG = LoggerFactory.getLogger(BrooklynRestApiLauncherTest.class);
    private static final String CHARSET_NAME = "UTF-8";
    private static final String HTTP_PORT = "9998";
    private static final String ROOT_URL = "http://localhost:" + HTTP_PORT;
    private static final String NO_SECURITY_KEY = "brooklyn.webconsole.security.provider";
    private static final String NO_SECURITY_VALUE = "org.apache.brooklyn.rest.security.provider.AnyoneSecurityProvider";
    private static final String TEST_POLICY = "org.apache.brooklyn.policy.ha.ServiceRestarter";
    private final UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("test", "test");

    @Inject
    protected ManagementContext managementContext;

    /**
     * To make sure the tests run only when the boot features are fully
     * installed
     */
    @Inject
    @Filter(timeout = 120000)
    BootFinished bootFinished;

    @Configuration
    public static Option[] configuration() throws Exception {
        return defaultOptionsWith(
                editConfigurationFilePut("etc/org.ops4j.pax.web.cfg", "org.osgi.service.http.port", HTTP_PORT),
                editConfigurationFilePut("etc/brooklyn.properties", NO_SECURITY_KEY, NO_SECURITY_VALUE),
                features(KarafTestUtils.brooklynFeaturesRepository(), "brooklyn-software-base")
                // Uncomment this for remote debugging the tests on port 5005
                // KarafDistributionOption.debugConfiguration()
        );
    }

    @Test
    public void testCatalogIsAvailable() throws Exception {
        final String testUrl = ROOT_URL + "/v1/catalog/entities";

        int code = Asserts.succeedsEventually(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                int code = HttpTool.getHttpStatusCode(testUrl, credentials);
                if (code == HttpStatus.SC_FORBIDDEN) {
                    throw new RuntimeException("Retry request");
                } else {
                    return code;
                }
            }
        });
        HttpAsserts.assertHealthyStatusCode(code);
        HttpAsserts.assertHealthyStatusCode(code);
    }

    @Test
    public void testCanAddApplication() throws Exception {
        final String testUrl = ROOT_URL + "/v1/applications";
        final String basicAuthToken = HttpTool.toBasicAuthorizationValue(credentials);
        final Map<String, String> headers = new HashMap();
        final HttpClient httpClient = HttpTool.httpClientBuilder().build();
        final String body = "name: Test Brooklyn EmptySoftwareProcess\n" +
                "services:\n" +
                "- type:           org.apache.brooklyn.entity.software.base.EmptySoftwareProcess\n" +
                "  name:           My VM\n" +
                "location: localhost";

        headers.put("authorization", basicAuthToken);
        headers.put("Accept", "application/json");
        headers.put("Content-Type", "application/json");

        HttpToolResponse response = HttpTool.httpPost(httpClient, Urls.toUri(testUrl), headers, body.getBytes(CHARSET_NAME));
        HttpAsserts.assertHealthyStatusCode(response.getResponseCode());
    }

    @Test
    public void testCanGetAllApplications() throws Exception {
        final String testUrl = ROOT_URL + "/v1/applications";

        ApplicationBuilder.newManagedApp(TestApplication.class, managementContext);

        final HttpURLConnection response = Asserts.succeedsEventually(new Callable<HttpURLConnection>() {
            @Override
            public HttpURLConnection call() throws Exception {
                final HttpURLConnection response = HttpTool.getHttpResponse(testUrl, credentials);
                if (response == null) {
                    throw new RuntimeException("Retry request");
                } else {
                    return response;
                }
            }
        });
        final String responseString = IOUtils.toString(response.getInputStream(), StandardCharsets.UTF_8);
        final int responseSize = new Gson().fromJson(new String(responseString), ArrayList.class).size();

        HttpAsserts.assertHealthyStatusCode(response.getResponseCode());
        Asserts.assertEquals(managementContext.getApplications().size(), responseSize);
    }

    @Test
    public void testCanAddPolicy() throws Exception {
        final TestApplication testApplication = ApplicationBuilder.newManagedApp(TestApplication.class, managementContext);

        final String testUrl = ROOT_URL + "/v1/applications/" + testApplication.getApplicationId() + "/entities/" + testApplication.getApplicationId() + "/policies?type="+TEST_POLICY;
        final String basicAuthToken = HttpTool.toBasicAuthorizationValue(credentials);
        final Map<String, String> headers = new HashMap();
        final HttpClient httpClient = HttpTool.httpClientBuilder().build();
        final String body = "";

        headers.put("authorization", basicAuthToken);
        headers.put("Accept", "application/json");
        headers.put("Content-Type", "application/json");

        HttpToolResponse response = HttpTool.httpPost(httpClient, Urls.toUri(testUrl), headers, body.getBytes(CHARSET_NAME));
        HttpAsserts.assertHealthyStatusCode(response.getResponseCode());
    }

}

