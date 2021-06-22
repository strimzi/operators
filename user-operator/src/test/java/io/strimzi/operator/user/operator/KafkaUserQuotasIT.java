/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.api.kafka.model.KafkaUserQuotas;
import io.strimzi.operator.common.DefaultAdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

@ExtendWith(VertxExtension.class)
public class KafkaUserQuotasIT {

    private static ZkClient zkClient;

    private static KafkaUserQuotasOperator kuq;

    private KafkaUserQuotas defaultQuotas;

    private static Vertx vertx;

    private static EmbeddedKafkaCluster kafkaCluster;

    @BeforeAll
    public static void beforeAll() {
        vertx = Vertx.vertx();

        try {
            kafkaCluster = new EmbeddedKafkaCluster(1);
            kafkaCluster.start();
        } catch (IOException e) {
            assertThat(false, is(true));
        }

        zkClient = new ZkClient(kafkaCluster.zKConnectString(), 6000_0, 30_000, new BytesPushThroughSerializer());

        kuq = new KafkaUserQuotasOperator(vertx,
                new DefaultAdminClientProvider().createAdminClient(kafkaCluster.bootstrapServers(), null, null, null));
    }

    @AfterAll
    public static void afterAll() {
        if (vertx != null) {
            vertx.close();
        }
    }

    @BeforeEach
    public void beforeEach() {
        defaultQuotas = new KafkaUserQuotas();
        defaultQuotas.setConsumerByteRate(1000);
        defaultQuotas.setProducerByteRate(2000);
        defaultQuotas.setControllerMutationRate(10d);
    }

    @Test
    public void testTlsUserExistsAfterCreate() throws Exception {
        testUserExistsAfterCreate("CN=userExists");
    }

    @Test
    public void testRegularUserExistsAfterCreate() throws Exception {
        testUserExistsAfterCreate("userExists");
    }

    public void testUserExistsAfterCreate(String username) throws Exception {
        assertThat(kuq.exists(Reconciliation.DUMMY_RECONCILIATION, username), is(false));
        kuq.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, username, defaultQuotas);
        assertThat(kuq.exists(Reconciliation.DUMMY_RECONCILIATION, username), is(true));
    }

    @Test
    public void testTlsUserDoesNotExistPriorToCreate() throws Exception {
        testUserDoesNotExistPriorToCreate("CN=userNotExists");
    }

    @Test
    public void testRegularUserDoesNotExistPriorToCreate() throws Exception {
        testUserDoesNotExistPriorToCreate("userNotExists");
    }

    public void testUserDoesNotExistPriorToCreate(String username) throws Exception {
        assertThat(kuq.exists(Reconciliation.DUMMY_RECONCILIATION, username), is(false));
    }

    @Test
    public void testCreateOrUpdateTlsUser() throws Exception {
        testUserQuotasNotExist("CN=tlsUser");
        testCreateOrUpdate("CN=tlsUser");
    }

    @Test
    public void testCreateOrUpdateRegularUser() throws Exception {
        testUserQuotasNotExist("user");
        testCreateOrUpdate("user");
    }

    @Test
    public void testCreateOrUpdateScramShaUser() throws Exception {
        createScramShaUser("scramShaUser", "scramShaPassword");
        testCreateOrUpdate("scramShaUser");
    }

    public void testCreateOrUpdate(String username) throws Exception {
        KafkaUserQuotas newQuotas = new KafkaUserQuotas();
        newQuotas.setConsumerByteRate(1000);
        newQuotas.setProducerByteRate(2000);
        newQuotas.setControllerMutationRate(10d);
        kuq.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, username, newQuotas);
        assertThat(isPathExist("/config/users/" + encodeUsername(username)), is(true));
        testDescribeUserQuotas(username, newQuotas);
    }

    @Test
    public void testCreateOrUpdateTwiceTlsUser() throws Exception {
        testCreateOrUpdateTwice("CN=doubleCreate");
    }

    @Test
    public void testCreateOrUpdateTwiceRegularUSer() throws Exception {
        testCreateOrUpdateTwice("doubleCreate");
    }

    public void testCreateOrUpdateTwice(String username) throws Exception {
        assertThat(isPathExist("/config/users/" + encodeUsername(username)), is(false));
        assertThat(kuq.describeUserQuotas(Reconciliation.DUMMY_RECONCILIATION, username), is(nullValue()));

        kuq.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, username, defaultQuotas);
        kuq.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, username, defaultQuotas);
        assertThat(isPathExist("/config/users/" + encodeUsername(username)), is(true));
        testDescribeUserQuotas(username, defaultQuotas);
    }

    @Test
    public void testDeleteTlsUser() throws Exception {
        testDelete("CN=normalDelete");
    }

    @Test
    public void testDeleteRegularUser() throws Exception {
        testDelete("normalDelete");
    }

    public void testDelete(String username) throws Exception {
        kuq.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, username, defaultQuotas);
        assertThat(isPathExist("/config/users/" + encodeUsername(username)), is(true));
        assertThat(kuq.exists(Reconciliation.DUMMY_RECONCILIATION, username), is(true));

        kuq.delete(Reconciliation.DUMMY_RECONCILIATION, username);
        assertThat(kuq.exists(Reconciliation.DUMMY_RECONCILIATION, username), is(false));
    }

    @Test
    public void testDeleteTwiceTlsUser() throws Exception {
        testDeleteTwice("CN=doubleDelete");
    }

    @Test
    public void testDeleteTwiceRegularUser() throws Exception {
        testDeleteTwice("doubleDelete");
    }

    public void testDeleteTwice(String username) throws Exception {
        kuq.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, username, defaultQuotas);
        assertThat(isPathExist("/config/users/" + encodeUsername(username)), is(true));
        assertThat(kuq.exists(Reconciliation.DUMMY_RECONCILIATION, username), is(true));

        kuq.delete(Reconciliation.DUMMY_RECONCILIATION, username);
        kuq.delete(Reconciliation.DUMMY_RECONCILIATION, username);
        assertThat(kuq.exists(Reconciliation.DUMMY_RECONCILIATION, username), is(false));
    }

    @Test
    public void testUpdateConsumerByteRate() throws Exception {
        String username = "changeConsumerByteRate";
        kuq.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, username, defaultQuotas);
        defaultQuotas.setConsumerByteRate(4000);
        kuq.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, username, defaultQuotas);
        assertThat(kuq.describeUserQuotas(Reconciliation.DUMMY_RECONCILIATION, username).getConsumerByteRate(), is(4000));
    }

    @Test
    public void testUpdateProducerByteRate() throws Exception {
        String username = "changeProducerByteRate";
        kuq.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, username, defaultQuotas);
        defaultQuotas.setProducerByteRate(8000);
        kuq.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, username, defaultQuotas);
        assertThat(kuq.describeUserQuotas(Reconciliation.DUMMY_RECONCILIATION, username).getProducerByteRate(), is(8000));
    }

    @Test
    public void testUpdateControllerMutationRate() throws Exception {
        String username = "changeControllerMutationRate";
        kuq.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, username, defaultQuotas);
        defaultQuotas.setControllerMutationRate(20d);
        kuq.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, username, defaultQuotas);
        assertThat(kuq.describeUserQuotas(Reconciliation.DUMMY_RECONCILIATION, username).getControllerMutationRate(), is(20d));
    }

    @Test
    public void testUserQuotasToClientQuotaAlterationOps() {
        KafkaUserQuotas quotas = new KafkaUserQuotas();
        quotas.setConsumerByteRate(2000);
        quotas.setProducerByteRate(4000);
        quotas.setRequestPercentage(40);
        quotas.setControllerMutationRate(10d);
        Set<ClientQuotaAlteration.Op> ops = kuq.toClientQuotaAlterationOps(quotas);
        assertThat(ops, hasSize(4));
        assertThat(ops.contains(new ClientQuotaAlteration.Op("consumer_byte_rate", 2000d)), is(true));
        assertThat(ops.contains(new ClientQuotaAlteration.Op("producer_byte_rate", 4000d)), is(true));
        assertThat(ops.contains(new ClientQuotaAlteration.Op("request_percentage", 40d)), is(true));
        assertThat(ops.contains(new ClientQuotaAlteration.Op("controller_mutation_rate", 10d)), is(true));

        quotas.setConsumerByteRate(null);
        quotas.setProducerByteRate(null);
        quotas.setRequestPercentage(null);
        quotas.setControllerMutationRate(null);

        ops = kuq.toClientQuotaAlterationOps(quotas);
        assertThat(ops, hasSize(4));
        assertThat(ops.contains(new ClientQuotaAlteration.Op("consumer_byte_rate", null)), is(true));
        assertThat(ops.contains(new ClientQuotaAlteration.Op("producer_byte_rate", null)), is(true));
        assertThat(ops.contains(new ClientQuotaAlteration.Op("request_percentage", null)), is(true));
        assertThat(ops.contains(new ClientQuotaAlteration.Op("controller_mutation_rate", null)), is(true));

    }

    @Test
    public void testClientQuotaAlterationOpsToUserQuotas() {
        Map<String, Double> map = new HashMap<>(3);
        map.put("consumer_byte_rate", 2000d);
        map.put("producer_byte_rate", 4000d);
        map.put("request_percentage", 40d);
        map.put("controller_mutation_rate", 10d);
        KafkaUserQuotas quotas = kuq.fromClientQuota(map);
        assertThat(quotas.getConsumerByteRate(), is(2000));
        assertThat(quotas.getProducerByteRate(), is(4000));
        assertThat(quotas.getRequestPercentage(), is(40));
        assertThat(quotas.getControllerMutationRate(), is(10d));

        map.remove("consumer_byte_rate");
        map.remove("producer_byte_rate");
        map.remove("request_percentage");
        map.remove("controller_mutation_rate");
        quotas = kuq.fromClientQuota(map);
        assertThat(quotas.getConsumerByteRate(), is(nullValue()));
        assertThat(quotas.getProducerByteRate(), is(nullValue()));
        assertThat(quotas.getRequestPercentage(), is(nullValue()));
        assertThat(quotas.getControllerMutationRate(), is(nullValue()));

    }

    @Test
    public void testReconcileCreatesTlsUserWithQuotas(VertxTestContext testContext) throws Exception  {
        testReconcileCreatesUserWithQuotas("CN=createTestUser", testContext);
    }

    @Test
    public void testReconcileCreatesRegularUserWithQuotas(VertxTestContext testContext) throws Exception  {
        testReconcileCreatesUserWithQuotas("createTestUser", testContext);
    }

    public void testReconcileCreatesUserWithQuotas(String username, VertxTestContext testContext) throws Exception  {
        KafkaUserQuotas quotas = new KafkaUserQuotas();
        quotas.setConsumerByteRate(2_000_000);
        quotas.setProducerByteRate(1_000_000);
        quotas.setRequestPercentage(50);
        quotas.setControllerMutationRate(10d);

        assertThat(kuq.exists(Reconciliation.DUMMY_RECONCILIATION, username), is(false));

        kuq.reconcile(Reconciliation.DUMMY_RECONCILIATION, username, quotas)
            .onComplete(testContext.succeeding(rr -> testContext.verify(() -> {
                assertThat(kuq.exists(Reconciliation.DUMMY_RECONCILIATION, username), is(true));
                assertThat(isPathExist("/config/users/" + encodeUsername(username)), is(true));
                testDescribeUserQuotas(username, quotas);
                testContext.completeNow();
            })));
    }

    @Test
    public void testReconcileUpdatesTlsUserQuotaValues(VertxTestContext testContext) throws Exception {
        testReconcileUpdatesUserQuotaValues("CN=updateTestUser", testContext);
    }

    @Test
    public void testReconcileUpdatesRegularUserQuotaValues(VertxTestContext testContext) throws Exception {
        testReconcileUpdatesUserQuotaValues("updateTestUser", testContext);
    }

    public void testReconcileUpdatesUserQuotaValues(String username, VertxTestContext testContext) throws Exception {
        KafkaUserQuotas initialQuotas = new KafkaUserQuotas();
        initialQuotas.setConsumerByteRate(2_000_000);
        initialQuotas.setProducerByteRate(1_000_000);
        initialQuotas.setRequestPercentage(50);
        initialQuotas.setControllerMutationRate(10d);

        kuq.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, username, initialQuotas);
        assertThat(kuq.exists(Reconciliation.DUMMY_RECONCILIATION, username), is(true));
        testDescribeUserQuotas(username, initialQuotas);

        KafkaUserQuotas updatedQuotas = new KafkaUserQuotas();
        updatedQuotas.setConsumerByteRate(4_000_000);
        updatedQuotas.setProducerByteRate(3_000_000);
        updatedQuotas.setRequestPercentage(75);
        updatedQuotas.setControllerMutationRate(10d);

        kuq.reconcile(Reconciliation.DUMMY_RECONCILIATION, username, updatedQuotas)
            .onComplete(testContext.succeeding(rr -> testContext.verify(() -> {
                assertThat(kuq.exists(Reconciliation.DUMMY_RECONCILIATION, username), is(true));
                assertThat(isPathExist("/config/users/" + encodeUsername(username)), is(true));
                testDescribeUserQuotas(username, updatedQuotas);
                testContext.completeNow();
            })));
    }

    @Test
    public void testReconcileUpdatesTlsUserQuotasWithFieldRemovals(VertxTestContext testContext) throws Exception {
        testReconcileUpdatesUserQuotasWithFieldRemovals("CN=updateTestUser", testContext);
    }

    @Test
    public void testReconcileUpdatesRegularUserQuotasWithFieldRemovals(VertxTestContext testContext) throws Exception {
        testReconcileUpdatesUserQuotasWithFieldRemovals("updateTestUser", testContext);
    }

    public void testReconcileUpdatesUserQuotasWithFieldRemovals(String username, VertxTestContext testContext) throws Exception {
        KafkaUserQuotas initialQuotas = new KafkaUserQuotas();
        initialQuotas.setConsumerByteRate(2_000_000);
        initialQuotas.setProducerByteRate(1_000_000);
        initialQuotas.setRequestPercentage(50);
        initialQuotas.setControllerMutationRate(10d);

        kuq.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, username, initialQuotas);
        assertThat(kuq.exists(Reconciliation.DUMMY_RECONCILIATION, username), is(true));
        testDescribeUserQuotas(username, initialQuotas);

        KafkaUserQuotas updatedQuotas = new KafkaUserQuotas();
        updatedQuotas.setConsumerByteRate(4_000_000);
        updatedQuotas.setProducerByteRate(3_000_000);
        updatedQuotas.setControllerMutationRate(20d);

        kuq.reconcile(Reconciliation.DUMMY_RECONCILIATION, username, updatedQuotas)
            .onComplete(testContext.succeeding(rr -> testContext.verify(() -> {
                assertThat(kuq.exists(Reconciliation.DUMMY_RECONCILIATION, username), is(true));
                assertThat(isPathExist("/config/users/" + encodeUsername(username)), is(true));
                testDescribeUserQuotas(username, updatedQuotas);
                testContext.completeNow();
            })));

    }

    @Test
    public void testReconcileDeletesTlsUserForNullQuota(VertxTestContext testContext) throws Exception {
        testReconcileDeletesUserForNullQuota("CN=deleteTestUser", testContext);
    }

    @Test
    public void testReconcileDeletesRegularUserForNullQuota(VertxTestContext testContext) throws Exception {
        testReconcileDeletesUserForNullQuota("deleteTestUser", testContext);
    }

    public void testReconcileDeletesUserForNullQuota(String username, VertxTestContext testContext) throws Exception {
        KafkaUserQuotas initialQuotas = new KafkaUserQuotas();
        initialQuotas.setConsumerByteRate(2_000_000);
        initialQuotas.setProducerByteRate(1_000_000);
        initialQuotas.setRequestPercentage(50);
        initialQuotas.setControllerMutationRate(10d);

        kuq.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, username, initialQuotas);
        assertThat(kuq.exists(Reconciliation.DUMMY_RECONCILIATION, username), is(true));
        testDescribeUserQuotas(username, initialQuotas);

        kuq.reconcile(Reconciliation.DUMMY_RECONCILIATION, username, null)
            .onComplete(testContext.succeeding(rr -> testContext.verify(() -> {
                assertThat(kuq.exists(Reconciliation.DUMMY_RECONCILIATION, username), is(false));
                testContext.completeNow();
            })));
    }

    private boolean isPathExist(String path) {
        return zkClient.exists(path);
    }

    private String encodeUsername(String username) {
        return URLEncoder.encode(username, StandardCharsets.UTF_8);
    }

    @SuppressWarnings("SameParameterValue")
    private void createScramShaUser(String username, String password) {
        // creating SCRAM-SHA user upfront to check it works because it shares same path in ZK as quotas
        ScramShaCredentials scramShaCred = new ScramShaCredentials(kafkaCluster.zKConnectString(), 6_000);
        scramShaCred.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, username, password);
        assertThat(scramShaCred.exists(username), is(true));
        assertThat(scramShaCred.isPathExist("/config/users/" + username), is(true));
    }

    private void testUserQuotasNotExist(String username) throws Exception {
        assertThat(kuq.describeUserQuotas(Reconciliation.DUMMY_RECONCILIATION, username), is(nullValue()));
        assertThat(isPathExist("/config/users/" + encodeUsername(username)), is(false));
    }

    private void testDescribeUserQuotas(String username, KafkaUserQuotas quotas) throws Exception {
        assertThat(kuq.describeUserQuotas(Reconciliation.DUMMY_RECONCILIATION, username), is(notNullValue()));
        assertThat(kuq.describeUserQuotas(Reconciliation.DUMMY_RECONCILIATION, username).getConsumerByteRate(), is(quotas.getConsumerByteRate()));
        assertThat(kuq.describeUserQuotas(Reconciliation.DUMMY_RECONCILIATION, username).getProducerByteRate(), is(quotas.getProducerByteRate()));
        assertThat(kuq.describeUserQuotas(Reconciliation.DUMMY_RECONCILIATION, username).getRequestPercentage(), is(quotas.getRequestPercentage()));
        assertThat(kuq.describeUserQuotas(Reconciliation.DUMMY_RECONCILIATION, username).getControllerMutationRate(), is(quotas.getControllerMutationRate()));
    }
}
