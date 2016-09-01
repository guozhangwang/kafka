/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.record;

import java.nio.ByteBuffer;

// TODO: we need to rename this class as it needs to sit in common package, not producer package
public class ProducerContext {

    // TODO: may need to rename this producer id of the record set
    private static final int PID_OFFSET = 0;
    private static final int PID_LENGTH = 8;

    // producer epoch number of the record set
    private static final int EPOCH_OFFSET = PID_OFFSET + PID_LENGTH;
    private static final int EPOCH_LENGTH = 4;

    // producer sequence number of the record set
    private static final int SEQUENCE_OFFSET = EPOCH_OFFSET + EPOCH_LENGTH;
    private static final int SEQUENCE_LENGTH = 8;

    // max delta offset of the record set
    private static final int SEQUENCE_DELTA_OFFSET = SEQUENCE_OFFSET + SEQUENCE_LENGTH;
    private static final int SEQUENCE_DELTA_LENGTH = 4;

    public static final int TOTAL_LENGTH = PID_LENGTH + EPOCH_LENGTH + SEQUENCE_LENGTH + SEQUENCE_DELTA_LENGTH;

    /**
     * Identifier of the producer
     */
    private final long id;

    /**
     * Current epoch of the producer
     */
    private final int epoch;

    /**
     * Starting sequence number of the producer within a record set
     */
    private final long sequence;

    /**
     * Delta sequence number of the producer within a record set
     */
    private final int sequenceDelta;

    public ProducerContext(long id, int epoch, long sequence, int sequenceDelta) {
        this.id = id;
        this.epoch = epoch;
        this.sequence = sequence;
        this.sequenceDelta = sequenceDelta;
    }

    public void write(ByteBuffer buffer, int index) {
        buffer.putLong(index + PID_OFFSET, id);
        buffer.putInt(index + EPOCH_OFFSET, epoch);
        buffer.putLong(index + SEQUENCE_OFFSET, sequence);
        buffer.putInt(index + SEQUENCE_DELTA_OFFSET, sequenceDelta);
    }

    public static ProducerContext read(ByteBuffer buffer, int index) {
        return new ProducerContext(
                buffer.getLong(index + PID_OFFSET),
                buffer.getInt(index + EPOCH_OFFSET),
                buffer.getLong(index + SEQUENCE_OFFSET),
                buffer.getInt(index + SEQUENCE_DELTA_OFFSET)
        );
    }
}
