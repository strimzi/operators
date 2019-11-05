/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.VersionInfo;
import io.strimzi.operator.common.Util;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


@ExtendWith(VertxExtension.class)
public class PlatformFeaturesAvailabilityTest {
    protected Vertx vertx;

    @BeforeEach
    public void before() {
        vertx = Vertx.vertx();
    }

    @AfterEach
    public void after() {
        vertx.close();
    }

    @Test
    public void networkPoliciesWithFancyCombinationTest() {
        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, KubernetesVersion.V1_8);
        assertThat(pfa.isNamespaceAndPodSelectorNetworkPolicySupported(), is(false));
        pfa = new PlatformFeaturesAvailability(true, KubernetesVersion.V1_11);
        assertThat(pfa.isNamespaceAndPodSelectorNetworkPolicySupported(), is(false));
        pfa = new PlatformFeaturesAvailability(false, KubernetesVersion.V1_11);
        assertThat(pfa.isNamespaceAndPodSelectorNetworkPolicySupported(), is(true));
        pfa = new PlatformFeaturesAvailability(true, KubernetesVersion.V1_12);
        assertThat(pfa.isNamespaceAndPodSelectorNetworkPolicySupported(), is(true));
    }

    @Test
    public void testVersionDetectionOpenShift39(VertxTestContext context) throws InterruptedException {
        String version = "{\n" +
                "  \"major\": \"1\",\n" +
                "  \"minor\": \"9\",\n" +
                "  \"gitVersion\": \"v1.9.1+a0ce1bc657\",\n" +
                "  \"gitCommit\": \"a0ce1bc\",\n" +
                "  \"gitTreeState\": \"clean\",\n" +
                "  \"buildDate\": \"2018-06-24T01:54:00Z\",\n" +
                "  \"goVersion\": \"go1.9\",\n" +
                "  \"compiler\": \"gc\",\n" +
                "  \"platform\": \"linux/amd64\"\n" +
                "}";

        HttpServer mockHttp = startMockApi(context, version, Collections.EMPTY_LIST);

        KubernetesClient client = new DefaultKubernetesClient("127.0.0.1:" + mockHttp.actualPort());
        Future<PlatformFeaturesAvailability> futurePfa = PlatformFeaturesAvailability.create(vertx, client);

        Checkpoint async = context.checkpoint();

        futurePfa.setHandler(res -> {
            if (res.succeeded())    {
                context.verify(() -> assertThat("Versions are not equal", res.result().getKubernetesVersion(), is(KubernetesVersion.V1_9)));
                async.flag();
            } else {
                context.failNow(new Throwable("Failed to create PlatformFeaturesAvailability object"));
                async.flag();
            }
        });

        context.awaitCompletion(60, TimeUnit.SECONDS);
        stopMockApi(context, mockHttp);
    }

    @Test
    public void testVersionDetectionMinikube114(VertxTestContext context) throws InterruptedException {
        String version = "{\n" +
                "  \"major\": \"1\",\n" +
                "  \"minor\": \"14\",\n" +
                "  \"gitVersion\": \"v1.14.0\",\n" +
                "  \"gitCommit\": \"641856db18352033a0d96dbc99153fa3b27298e5\",\n" +
                "  \"gitTreeState\": \"clean\",\n" +
                "  \"buildDate\": \"2019-03-25T15:45:25Z\",\n" +
                "  \"goVersion\": \"go1.12.1\",\n" +
                "  \"compiler\": \"gc\",\n" +
                "  \"platform\": \"linux/amd64\"\n" +
                "}";

        HttpServer mockHttp = startMockApi(context, version, Collections.EMPTY_LIST);

        KubernetesClient client = new DefaultKubernetesClient("127.0.0.1:" + mockHttp.actualPort());
        Future<PlatformFeaturesAvailability> futurePfa = PlatformFeaturesAvailability.create(vertx, client);

        Checkpoint async = context.checkpoint();

        futurePfa.setHandler(res -> {
            if (res.succeeded())    {
                context.verify(() -> assertThat("Versions are not equal", res.result().getKubernetesVersion(), is(KubernetesVersion.V1_14)));
                async.flag();
            } else {
                context.failNow(new Throwable("Failed to create PlatformFeaturesAvailability object"));
                async.flag();
            }
        });

        context.awaitCompletion(60, TimeUnit.SECONDS);
        stopMockApi(context, mockHttp);
    }

    @Test
    public void testApiDetectionOce(VertxTestContext context) throws InterruptedException {
        List<String> apis = new ArrayList<>();
        apis.add("/apis/route.openshift.io/v1");
        apis.add("/apis/build.openshift.io/v1");

        HttpServer mockHttp = startMockApi(context, apis);

        KubernetesClient client = new DefaultKubernetesClient("127.0.0.1:" + mockHttp.actualPort());
        Future<PlatformFeaturesAvailability> futurePfa = PlatformFeaturesAvailability.create(vertx, client);

        Checkpoint async = context.checkpoint();

        futurePfa.setHandler(res -> {
            if (res.succeeded())    {
                PlatformFeaturesAvailability pfa = res.result();
                context.verify(() -> assertThat(pfa.hasRoutes(), is(true)));
                context.verify(() -> assertThat(pfa.hasBuilds(), is(true)));
                context.verify(() -> assertThat(pfa.hasImages(), is(false)));
                context.verify(() -> assertThat(pfa.hasApps(), is(false)));
                async.flag();
            } else {
                context.failNow(new Throwable("Failed to create PlatformFeaturesAvailability object"));
                async.flag();
            }
        });

        context.awaitCompletion(60, TimeUnit.SECONDS);
        stopMockApi(context, mockHttp);
    }

    @Test
    public void testApiDetectionOpenshift(VertxTestContext context) throws InterruptedException {
        List<String> apis = new ArrayList<>();
        apis.add("/apis/route.openshift.io/v1");
        apis.add("/apis/build.openshift.io/v1");
        apis.add("/apis/apps.openshift.io/v1");
        apis.add("/apis/image.openshift.io/v1");

        HttpServer mockHttp = startMockApi(context, apis);

        KubernetesClient client = new DefaultKubernetesClient("127.0.0.1:" + mockHttp.actualPort());
        Future<PlatformFeaturesAvailability> futurePfa = PlatformFeaturesAvailability.create(vertx, client);

        Checkpoint async = context.checkpoint();

        futurePfa.setHandler(res -> {
            if (res.succeeded())    {
                PlatformFeaturesAvailability pfa = res.result();
                context.verify(() -> assertThat(pfa.hasRoutes(), is(true)));
                context.verify(() -> assertThat(pfa.hasBuilds(), is(true)));
                context.verify(() -> assertThat(pfa.hasImages(), is(true)));
                context.verify(() -> assertThat(pfa.hasApps(), is(true)));
                async.flag();
            } else {
                context.failNow(new Throwable("Failed to create PlatformFeaturesAvailability object"));
                async.flag();
            }
        });

        context.awaitCompletion(60, TimeUnit.SECONDS);
        stopMockApi(context, mockHttp);
    }

    @Test
    public void testApiDetectionKubernetes(VertxTestContext context) throws InterruptedException {
        HttpServer mockHttp = startMockApi(context, Collections.EMPTY_LIST);

        KubernetesClient client = new DefaultKubernetesClient("127.0.0.1:" + mockHttp.actualPort());
        Future<PlatformFeaturesAvailability> futurePfa = PlatformFeaturesAvailability.create(vertx, client);

        Checkpoint async = context.checkpoint();

        futurePfa.setHandler(res -> {
            if (res.succeeded())    {
                PlatformFeaturesAvailability pfa = res.result();
                context.verify(() -> assertThat(pfa.hasRoutes(), is(false)));
                context.verify(() -> assertThat(pfa.hasBuilds(), is(false)));
                context.verify(() -> assertThat(pfa.hasImages(), is(false)));
                context.verify(() -> assertThat(pfa.hasApps(), is(false)));
                async.flag();
            } else {
                context.failNow(new Throwable("Failed to create PlatformFeaturesAvailability object"));
                async.flag();
            }
        });

        context.awaitCompletion(60, TimeUnit.SECONDS);
        stopMockApi(context, mockHttp);
    }

    @Test
    public void versionInfoFromMap(VertxTestContext context) throws ParseException {
        String version =  "major=1\n" +
                "minor=16\n" +
                "gitVersion=v1.16.2\n" +
                "gitCommit=c97fe5036ef3df2967d086711e6c0c405941e14b\n" +
                "gitTreeState=clean\n" +
                "buildDate=2019-10-15T19:09:08Z\n" +
                "goVersion=go1.12.10\n" +
                "compiler=gc\n" +
                "platform=linux/amd64";

        VersionInfo vi = new VersionInfo(Util.parseMap(version));

        context.verify(() -> assertThat(vi.getMajor(), is("1")));
        context.verify(() -> assertThat(vi.getMinor(), is("16")));
    }

    public HttpServer startMockApi(VertxTestContext context, String version, List<String> apis) throws InterruptedException {
        Checkpoint start = context.checkpoint();

        HttpServer server = vertx.createHttpServer().requestHandler(request -> {
            if (HttpMethod.GET.equals(request.method()) && apis.contains(request.uri()))   {
                request.response().setStatusCode(200).end();
            } else if (HttpMethod.GET.equals(request.method()) && "/version".equals(request.uri())) {
                request.response().setStatusCode(200).end(version);
            } else {
                request.response().setStatusCode(404).end();
            }
        }).listen(0, res -> {
            if (res.succeeded())    {
                start.flag();
            } else {
                throw new RuntimeException(res.cause());
            }
        });

        context.awaitCompletion(60, TimeUnit.SECONDS);

        return server;
    }

    public HttpServer startMockApi(VertxTestContext context, List<String> apis) throws InterruptedException {
        String version = "{\n" +
                "  \"major\": \"1\",\n" +
                "  \"minor\": \"9\",\n" +
                "  \"gitVersion\": \"v1.9.1+a0ce1bc657\",\n" +
                "  \"gitCommit\": \"a0ce1bc\",\n" +
                "  \"gitTreeState\": \"clean\",\n" +
                "  \"buildDate\": \"2018-06-24T01:54:00Z\",\n" +
                "  \"goVersion\": \"go1.9\",\n" +
                "  \"compiler\": \"gc\",\n" +
                "  \"platform\": \"linux/amd64\"\n" +
                "}";

        return startMockApi(context, version, apis);
    }

    public void stopMockApi(VertxTestContext context, HttpServer server) throws InterruptedException {
        Checkpoint stop = context.checkpoint();

        server.close(res -> {
            if (res.succeeded())    {
                stop.flag();
            } else {
                throw new RuntimeException("Failed to stop Mock HTTP server");
            }
        });

        context.awaitCompletion(60, TimeUnit.SECONDS);
    }
}
