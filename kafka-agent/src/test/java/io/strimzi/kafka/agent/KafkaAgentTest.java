/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.agent;

import com.yammer.metrics.core.Gauge;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaAgentTest {
    private Server server;
    private ContextHandler context;
    private HttpRequest req;

    @Before
    public void setUp() throws URISyntaxException {
        server = new Server();
        ServerConnector conn = new ServerConnector(server);
        conn.setPort(8080);
        server.setConnectors(new Connector[] {conn});
        context = new ContextHandler("/");

        req = HttpRequest.newBuilder()
                .uri(new URI("http://localhost:8080/"))
                .GET()
                .build();
    }

    @After
    public void tearDown() throws Exception {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void testBrokerRunningState() throws Exception {
        final Gauge brokerState = mock(Gauge.class);
        when(brokerState.value()).thenReturn((byte) 3);
        KafkaAgent agent = new KafkaAgent(brokerState, null, null);
        context.setHandler(agent.getServerHandler());
        server.setHandler(context);
        server.start();

        HttpResponse<String> response = HttpClient.newBuilder()
                .build()
                .send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(response.statusCode(), HttpServletResponse.SC_OK);

        String expectedResponse = "{\"brokerState\":3}";
        assertEquals(expectedResponse, response.body());
    }

    @Test
    public void testBrokerRecoveryState() throws Exception {
        final Gauge brokerState = mock(Gauge.class);
        when(brokerState.value()).thenReturn((byte) 2);

        final Gauge remainingLogs = mock(Gauge.class);
        when(remainingLogs.value()).thenReturn((byte) 10);

        final Gauge remainingSegments = mock(Gauge.class);
        when(remainingSegments.value()).thenReturn((byte) 100);

        KafkaAgent agent = new KafkaAgent(brokerState, remainingLogs, remainingSegments);
        context.setHandler(agent.getServerHandler());
        server.setHandler(context);
        server.start();

        HttpResponse<String> response = HttpClient.newBuilder()
                .build()
                .send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(response.statusCode(), HttpServletResponse.SC_OK);

        String expectedResponse = "{\"brokerState\":2,\"recoveryState\":{\"remainingLogsToRecover\":10,\"remainingSegmentsToRecover\":100}}";
        assertEquals(expectedResponse, response.body());
    }

    @Test
    public void testBrokerMetricNotFound() throws Exception {
        KafkaAgent agent = new KafkaAgent(null, null, null);
        context.setHandler(agent.getServerHandler());
        server.setHandler(context);
        server.start();

        HttpResponse<String> response = HttpClient.newBuilder()
                .build()
                .send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(HttpServletResponse.SC_NOT_FOUND, response.statusCode());

    }

}
