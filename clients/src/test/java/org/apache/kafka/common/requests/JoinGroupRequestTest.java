/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class JoinGroupRequestTest {

    @Test
    public void shouldAcceptValidGroupInstanceIds() {
        String maxLengthString = TestUtils.randomString(249);
        String[] validGroupInstanceIds = {"valid", "INSTANCE", "gRoUp", "ar6", "VaL1d", "_0-9_.", "...", maxLengthString};

        for (String instanceId : validGroupInstanceIds) {
            JoinGroupRequest.validateGroupInstanceId(instanceId);
        }
    }

    @Test
    public void shouldThrowOnInvalidGroupInstanceIds() {
        char[] longString = new char[250];
        Arrays.fill(longString, 'a');
        String[] invalidGroupInstanceIds = {"", "foo bar", "..", "foo:bar", "foo=bar", ".", new String(longString)};

        for (String instanceId : invalidGroupInstanceIds) {
            try {
                JoinGroupRequest.validateGroupInstanceId(instanceId);
                fail("No exception was thrown for invalid instance id: " + instanceId);
            } catch (InvalidConfigurationException e) {
                // Good
            }
        }
    }

    @Test
    public void shouldRecognizeInvalidCharactersInGroupInstanceIds() {
        char[] invalidChars = {'/', '\\', ',', '\u0000', ':', '"', '\'', ';', '*', '?', ' ', '\t', '\r', '\n', '='};

        for (char c : invalidChars) {
            String instanceId = "Is " + c + "illegal";
            assertFalse(JoinGroupRequest.containsValidPattern(instanceId));
        }
    }
}
