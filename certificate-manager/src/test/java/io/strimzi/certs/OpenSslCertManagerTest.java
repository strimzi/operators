/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.Principal;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

public class OpenSslCertManagerTest {

    private static CertificateFactory certFactory;
    private static CertManager ssl;

    @BeforeAll
    public static void before() throws CertificateException {
        assertThat(System.getProperty("os.name").contains("nux"), is(true));
        certFactory = CertificateFactory.getInstance("X.509");
        ssl = new OpenSslCertManager();
    }

    @Test
    public void testGenerateSelfSignedCert() throws Exception {
        File key = File.createTempFile("key-", ".key");
        File cert = File.createTempFile("crt-", ".crt");
        File store = File.createTempFile("crt-", ".str");

        testGenerateSelfSignedCert(key, cert, store, "123456", null);

        key.delete();
        cert.delete();
        store.delete();
    }

    @Test
    public void testGenerateSelfSignedCertWithSubject() throws Exception {

        File key = File.createTempFile("key-", ".key");
        File cert = File.createTempFile("crt-", ".crt");
        File store = File.createTempFile("crt-", ".str");
        Subject sbj = new Subject();
        sbj.setCommonName("MyCommonName");
        sbj.setOrganizationName("MyOrganization");

        testGenerateSelfSignedCert(key, cert, store, "123456", sbj);

        key.delete();
        cert.delete();
        store.delete();
    }

    @Test
    public void testGenerateSelfSignedCertWithSubjectAndAltNames() throws Exception {

        File key = File.createTempFile("key-", ".key");
        File cert = File.createTempFile("crt-", ".crt");
        File store = File.createTempFile("crt-", ".str");
        Subject sbj = new Subject();
        sbj.setCommonName("MyCommonName");
        sbj.setOrganizationName("MyOrganization");
        Map<String, String> subjectAltNames = new HashMap<>();
        subjectAltNames.put("DNS.1", "example1.com");
        subjectAltNames.put("DNS.2", "example2.com");
        sbj.setSubjectAltNames(subjectAltNames);

        testGenerateSelfSignedCert(key, cert, store, "123456", sbj);

        key.delete();
        cert.delete();
        store.delete();
    }

    private void testGenerateSelfSignedCert(File key, File cert, File trustStore, String trustStorePassword, Subject sbj) throws Exception {
        ssl.generateSelfSignedCert(key, cert, sbj, 365);
        ssl.addCertToTrustStore(cert, "ca", trustStore, trustStorePassword);

        Certificate c = certFactory.generateCertificate(new FileInputStream(cert));

        c.verify(c.getPublicKey());

        // subject verification if provided
        if (sbj != null) {
            if (c instanceof X509Certificate) {
                X509Certificate x509Certificate = (X509Certificate) c;
                Principal p = x509Certificate.getSubjectDN();

                assertThat(String.format("CN=%s, O=%s", sbj.commonName(), sbj.organizationName()), is(p.getName()));

                if (sbj.subjectAltNames() != null && sbj.subjectAltNames().size() > 0) {
                    final Collection<List<?>> sans = x509Certificate.getSubjectAlternativeNames();
                    assertThat(sans, is(notNullValue()));
                    assertThat(sbj.subjectAltNames().size(), is(sans.size()));
                    for (final List<?> sanItem : sans) {
                        assertThat(sbj.subjectAltNames().containsValue(sanItem.get(1)), is(true));
                    }
                }
            } else {
                fail();
            }
        }

        // truststore verification if provided
        if (trustStore != null) {
            KeyStore store = KeyStore.getInstance("PKCS12");
            store.load(new FileInputStream(trustStore), trustStorePassword.toCharArray());
            X509Certificate storeCert = (X509Certificate) store.getCertificate("ca");
            storeCert.verify(storeCert.getPublicKey());
        }
    }

    @Test
    public void testGenerateSignedCert() throws Exception {

        Path path = Files.createTempDirectory(OpenSslCertManagerTest.class.getSimpleName());
        path.toFile().deleteOnExit();
        long fileCount = Files.list(path).count();
        File caKey = File.createTempFile("ca-key-", ".key");
        File caCert = File.createTempFile("ca-crt-", ".crt");

        Subject caSbj = new Subject();
        caSbj.setCommonName("CACommonName");
        caSbj.setOrganizationName("CAOrganizationName");

        File key = File.createTempFile("key-", ".key");
        File csr = File.createTempFile("csr-", ".csr");
        Subject sbj = new Subject();
        sbj.setCommonName("MyCommonName");
        sbj.setOrganizationName("MyOrganization");
        File cert = File.createTempFile("crt-", ".crt");

        testGenerateSignedCert(caKey, caCert, caSbj, key, csr, cert, sbj);

        caKey.delete();
        caCert.delete();
        key.delete();
        csr.delete();
        cert.delete();

        assertThat(Files.list(path).count(), is(fileCount));
    }

    @Test
    public void testGenerateSignedCertWithSubjectAndAltNames() throws Exception {

        File caKey = File.createTempFile("ca-key-", ".key");
        File caCert = File.createTempFile("ca-crt-", ".crt");

        Subject caSbj = new Subject();
        caSbj.setCommonName("CACommonName");
        caSbj.setOrganizationName("CAOrganizationName");

        File key = File.createTempFile("key-", ".key");
        File csr = File.createTempFile("csr-", ".csr");
        Subject sbj = new Subject();
        sbj.setCommonName("MyCommonName");
        sbj.setOrganizationName("MyOrganization");
        Map<String, String> subjectAltNames = new HashMap<>();
        subjectAltNames.put("DNS.1", "example1.com");
        subjectAltNames.put("DNS.2", "example2.com");
        sbj.setSubjectAltNames(subjectAltNames);

        File cert = File.createTempFile("crt-", ".crt");

        testGenerateSignedCert(caKey, caCert, caSbj, key, csr, cert, sbj);

        caKey.delete();
        caCert.delete();
        key.delete();
        csr.delete();
        cert.delete();
    }

    private void testGenerateSignedCert(File caKey, File caCert, Subject caSbj, File key, File csr, File cert, Subject sbj) throws Exception {

        ssl.generateSelfSignedCert(caKey, caCert, caSbj, 365);

        ssl.generateCsr(key, csr, sbj);

        ssl.generateCert(csr, caKey, caCert, cert, sbj, 365);

        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        Certificate c = cf.generateCertificate(new FileInputStream(cert));
        Certificate ca = cf.generateCertificate(new FileInputStream(caCert));

        c.verify(ca.getPublicKey());

        if (c instanceof X509Certificate) {
            X509Certificate x509Certificate = (X509Certificate) c;
            Principal p = x509Certificate.getSubjectDN();

            assertThat(String.format("CN=%s, O=%s", sbj.commonName(), sbj.organizationName()), is(p.getName()));

            if (sbj != null && sbj.subjectAltNames() != null && sbj.subjectAltNames().size() > 0) {
                final Collection<List<?>> snas = x509Certificate.getSubjectAlternativeNames();
                if (snas != null) {
                    for (final List<?> sanItem : snas) {
                        assertThat(sbj.subjectAltNames().containsValue(sanItem.get(1)), is(true));
                    }
                } else {
                    fail();
                }
            }
        } else {
            fail();
        }
    }

    @Test
    public void testRenewSelfSignedCertWithSubject() throws Exception {
        // First generate a self-signed cert
        File caKey = File.createTempFile("key-", ".key");
        File originalCert = File.createTempFile("crt-", ".crt");
        File originalStore = File.createTempFile("crt-", ".str");
        Subject caSubject = new Subject();
        caSubject.setCommonName("MyCommonName");
        caSubject.setOrganizationName("MyOrganization");

        testGenerateSelfSignedCert(caKey, originalCert, originalStore, "123456", caSubject);

        // generate a client cert
        File clientKey = File.createTempFile("client-", ".key");
        File csr = File.createTempFile("client-", ".csr");
        File clientCert = File.createTempFile("client-", ".crt");
        Subject clientSubject = new Subject();
        clientSubject.setCommonName("MyCommonName");
        clientSubject.setOrganizationName("MyOrganization");
        ssl.generateCsr(clientKey, csr, clientSubject);

        ssl.generateCert(csr, caKey, originalCert, clientCert, clientSubject, 365);
        csr.delete();
        originalCert.delete();
        originalStore.delete();

        // Generate a renewed CA certificate
        File newCert = File.createTempFile("crt-", ".crt");
        File newStore = File.createTempFile("crt-", ".str");
        ssl.renewSelfSignedCert(caKey, newCert, caSubject, 365);
        ssl.addCertToTrustStore(newCert, "ca", newStore, "123456");

        // verify the client cert is valid wrt the new cert.
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        Certificate c = cf.generateCertificate(new FileInputStream(clientCert));
        Certificate ca = cf.generateCertificate(new FileInputStream(newCert));

        c.verify(ca.getPublicKey());

        clientKey.delete();
        clientCert.delete();

        caKey.delete();
        newCert.delete();
        newStore.delete();
    }
}
