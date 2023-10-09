/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security;

import io.strimzi.certs.Subject;
import io.strimzi.test.TestUtils;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Base64;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OpenSsl {
    private static final Logger LOGGER = LogManager.getLogger(OpenSsl.class);

    private static class OpenSslCommand {
        ProcessBuilder pb = new ProcessBuilder();

        public OpenSslCommand(String command) {
            this("openssl", command);
        }

        public OpenSslCommand(String binary, String command) {
            pb.command().add(binary);
            pb.command().add(command);
        }

        public OpenSslCommand withOption(String option) {
            pb.command().add(option);
            return this;
        }

        public OpenSslCommand withOptionAndArgument(String option, File argument) {
            pb.command().add(option);
            pb.command().add(argument.getAbsolutePath());
            return this;
        }

        public OpenSslCommand withOptionAndArgument(String option, String argument) {
            pb.command().add(option);
            pb.command().add(argument);
            return this;
        }

        public OpenSslCommand withOptionAndArgument(String option, Subject subject) {
            pb.command().add(option);
            pb.command().add(subject.opensslDn());
            return this;
        }

        public void execute() {
            execute(true);
        }

        public void execute(boolean failOnNonZeroOutput) {
            try {
                LOGGER.info("Running command: {}", pb.command());

                Process process = pb.start();
                int exitCode = process.waitFor();

                String line;
                BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
                StringBuilder output = new StringBuilder();

                while ((line = reader.readLine()) != null) {
                    output.append("\n" + line);
                }

                if (exitCode != 0 && failOnNonZeroOutput) {
                    throw new RuntimeException("openssl exit code " + exitCode);
                }
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static File generatePrivateKey() {
        return generatePrivateKey(2048);
    }

    public static File generatePrivateKey(int keyLengthBits) {
        try {
            LOGGER.info("Creating client rsa private key with size of {} bits", keyLengthBits);
            File privateKey = Files.createTempFile("private-key-", ".pem").toFile();

            new OpenSslCommand("genpkey")
                    .withOptionAndArgument("-algorithm", "RSA")
                    .withOptionAndArgument("-pkeyopt", "rsa_keygen_bits:" + keyLengthBits)
                    .withOptionAndArgument("-out", privateKey)
                    .execute();

            return privateKey;
        } catch (IOException e)  {
            throw new RuntimeException(e);
        }
    }

    public static File generateCertSigningRequest(File privateKey, Subject subject) {
        try {
            LOGGER.info("Creating Certificate Signing Request file");
            File csr = Files.createTempFile("csr-", ".pem").toFile();

            new OpenSslCommand("req")
                .withOption("-new")
                .withOptionAndArgument("-key", privateKey)
                .withOptionAndArgument("-out", csr)
                .withOptionAndArgument("-subj", subject)
                .execute();

            return csr;
        } catch (IOException e)  {
            throw new RuntimeException(e);
        }
    }

    public static File generateSignedCert(File csr, File caCrt, File caKey) {
        try {
            LOGGER.info("Creating signed certificate file");
            File cert = Files.createTempFile("signed-cert-", ".pem").toFile();

            new OpenSslCommand("x509")
                .withOption("-req")
                .withOptionAndArgument("-in", csr)
                .withOptionAndArgument("-CA", caCrt)
                .withOptionAndArgument("-CAkey", caKey)
                .withOptionAndArgument("-out", cert)
                .execute();

            return cert;
        } catch (IOException e)  {
            throw new RuntimeException(e);
        }
    }
}
