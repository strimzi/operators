/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.ConfigMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class FileUtils {

    private static final Logger LOGGER = LogManager.getLogger(FileUtils.class);

    private FileUtils() { }

    @SuppressFBWarnings({"RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE", "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE"})
    public static File downloadAndUnzip(String url) throws IOException {
        File dir = Files.createTempDirectory(FileUtils.class.getName()).toFile();

        try (InputStream bais = (InputStream) URI.create(url).toURL().openConnection().getContent();
            ZipInputStream zin = new ZipInputStream(bais)) {

            dir.deleteOnExit();

            ZipEntry entry = zin.getNextEntry();
            byte[] buffer = new byte[8 * 1024];
            int len;
            while (entry != null) {
                File file = new File(dir, entry.getName());

                if (!file.toPath().normalize().startsWith(dir.toPath())) {
                    throw new RuntimeException("Invalid zip entry - unpacks outside of the destination path");
                }

                if (entry.isDirectory()) {
                    if (file.exists()) {
                        if (!file.isDirectory()) {
                            throw new IOException("Malformed zip file");
                        }
                    } else {
                        if (!file.mkdirs()) {
                            throw new IOException("Could not create directory " + file);
                        }
                    }
                } else {
                    file.getParentFile().mkdirs(); // create parent; in case zip file is malformed
                    try (FileOutputStream fout = new FileOutputStream(file)) {
                        while ((len = zin.read(buffer)) != -1) {
                            fout.write(buffer, 0, len);
                        }
                    }
                }
                entry = zin.getNextEntry();
            }
        } catch (RuntimeException e) {
            LOGGER.error("RuntimeException {}", e.getMessage());
        }
        return dir;
    }

    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public static File downloadYaml(String url) throws IOException {
        File yamlFile = Files.createTempFile("temp-file", ".yaml").toFile();

        try (InputStream bais = (InputStream) URI.create(url).toURL().openConnection().getContent();
             BufferedReader br = new BufferedReader(new InputStreamReader(bais, StandardCharsets.UTF_8));
             OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(yamlFile), StandardCharsets.UTF_8)) {

            StringBuilder sb = new StringBuilder();

            String read;
            while ((read = br.readLine()) != null) {
                sb.append(read);
                sb.append("\n");
            }
            String yaml = sb.toString();

            osw.write(yaml);
            return yamlFile;

        } catch (RuntimeException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static File updateNamespaceOfYamlFile(String namespaceName, String pathToOrigin) throws IOException {
        byte[] encoded;
        File yamlFile = Files.createTempFile("temp-file", ".yaml").toFile();

        try (OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(yamlFile), StandardCharsets.UTF_8)) {
            encoded = Files.readAllBytes(Paths.get(pathToOrigin));

            String yaml = new String(encoded, StandardCharsets.UTF_8);
            yaml = yaml.replaceAll("myproject", namespaceName);

            osw.write(yaml);
            return yamlFile.toPath().toFile();
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Loads list of YAML documents from a file and extracts a ConfigMap with given name from it.
     *
     * @param yamlPath  Path to the YAML file
     * @param name      Name of the ConfigMap that should be extracted
     *
     * @return  Extracted ConfigMap
     */
    public static ConfigMap extractConfigMapFromYAMLWithResources(String yamlPath, String name) {
        try {
            YAMLFactory yaml = new YAMLFactory();
            ObjectMapper mapper = new ObjectMapper(yaml);
            YAMLParser yamlParser = yaml.createParser(new File(yamlPath));
            List<ConfigMap> list = mapper.readValues(yamlParser, new TypeReference<ConfigMap>() { }).readAll();
            Optional<ConfigMap> cmOpt = list.stream().filter(cm -> "ConfigMap".equals(cm.getKind()) && name.equals(cm.getMetadata().getName())).findFirst();
            if (cmOpt.isPresent()) {
                return cmOpt.get();
            } else {
                LOGGER.warn("ConfigMap {} not found in file {}", name, yamlPath);
                return null;
            }
        } catch (InvalidFormatException e) {
            throw new IllegalArgumentException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
