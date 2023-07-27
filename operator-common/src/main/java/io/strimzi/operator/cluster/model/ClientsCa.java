/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.common.CertificateExpirationPolicy;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;

/**
 * Represents the Strimzi Clients CA
 */
public class ClientsCa extends Ca {
    private Secret brokersSecret;

    /**
     * Creates a ClientsCA instance
     *
     * @param reconciliation        Reconciliation marker
     * @param certManager           Certificate manager instance
     * @param passwordGenerator     Password generator instance
     * @param caCertSecretName      Name of the Kubernetes Secret where the Clients CA public key wil be stored
     * @param clientsCaCert         Kubernetes Secret where the Clients CA public key will be stored
     * @param caSecretKeyName       Name of the Kubernetes Secret where the Clients CA private key wil be stored
     * @param clientsCaKey          Kubernetes Secret where the Clients CA private key will be stored
     * @param validityDays          Number of days for which the Clients CA certificate should be value
     * @param renewalDays           Number of day before expiration, when the certificate should be renewed
     * @param generateCa            Flag indicating whether the Clients CA should be generated by Strimzi or not
     * @param policy                Policy defining the behavior when the Clients CA expires (renewal or completely replacing the Clients CA)
     */
    public ClientsCa(Reconciliation reconciliation, CertManager certManager, PasswordGenerator passwordGenerator, String caCertSecretName, Secret clientsCaCert,
                     String caSecretKeyName, Secret clientsCaKey,
                     int validityDays, int renewalDays, boolean generateCa, CertificateExpirationPolicy policy) {
        super(reconciliation, certManager, passwordGenerator,
                "clients-ca", caCertSecretName,
                clientsCaCert, caSecretKeyName,
                clientsCaKey, validityDays, renewalDays, generateCa, policy);
    }

    /**
     * Passes the current broker Secret during reconciliation
     *
     * @param secret    Kubernetes Secret
     */
    public void initBrokerSecret(Secret secret) {
        this.brokersSecret = secret;
    }

    @Override
    protected String caCertGenerationAnnotation() {
        return ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION;
    }

    @Override
    protected boolean hasCaCertGenerationChanged() {
        return hasCaCertGenerationChanged(brokersSecret);
    }

    @Override
    public String toString() {
        return "clients-ca";
    }
}
