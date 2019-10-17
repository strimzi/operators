/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.test.TestUtils;
import org.junit.Test;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Member;
import java.util.List;

import static org.junit.Assert.assertNotNull;

public class ResourceVisitorTest {

    @Test
    public void testDoesNotThrow() {
        Kafka k = TestUtils.fromYaml("/example.yaml", Kafka.class, true);
        assertNotNull(k);
        ResourceVisitor.visit(k, new ResourceVisitor.Visitor() {
            @Override
            public <M extends AnnotatedElement & Member> void visitProperty(List<String> path, Object owner, M member, ResourceVisitor.Property<M> property, Object propertyValue) {

            }

            @Override
            public void visitObject(List<String> path, Object object) {

            }
        });
    }

}
