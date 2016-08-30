/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
public class RecordTest {

    private byte[] key;
    private byte[] value;
    private Record record;

    public RecordTest(long timestamp, byte[] key, byte[] value) {
        this.key = key == null ? null : key;
        this.value = value == null ? null : value;
        this.record = new Record(timestamp, 0, key, value);
    }

    @Test
    public void testFields() {
        assertEquals(key != null, record.hasKey());
        assertEquals(key, record.key());
        if (key != null)
            assertEquals(key.length, record.keySize());
        assertEquals(value, record.value());
        if (value != null)
            assertEquals(value.length, record.valueSize());
    }

    @Parameters
    public static Collection<Object[]> data() {
        byte[] payload = new byte[1000];
        Arrays.fill(payload, (byte) 1);
        List<Object[]> values = new ArrayList<>();
        for (long timestamp : Arrays.asList(Record.NO_TIMESTAMP, 0L, 1L))
            for (byte[] key : Arrays.asList(null, "".getBytes(), "key".getBytes(), payload))
                for (byte[] value : Arrays.asList(null, "".getBytes(), "value".getBytes(), payload))
                    for (CompressionType compression : CompressionType.values())
                        values.add(new Object[] {timestamp, key, value, compression});
        return values;
    }

}
