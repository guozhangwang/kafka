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

/**
 * Binary format of a set of {@link Record}s along with the record set metadata. See {@link MemoryRecords}
 * for the in-memory representation.
 */
public interface Records extends Iterable<LogEntry> {

    /**
     * The current offset and size for all the fixed-length fields
     */

    // starting offset of the record set
    int OFFSET_OFFSET = 0;
    int OFFSET_LENGTH = 8;

    // record set size
    int SIZE_OFFSET = OFFSET_LENGTH;
    int SIZE_LENGTH = 4;

    // crc of the record set
    int CRC_OFFSET = SIZE_OFFSET + SIZE_LENGTH;
    int CRC_LENGTH = 4;

    // magic byte of the record set
    int MAGIC_OFFSET = CRC_OFFSET + CRC_LENGTH;
    int MAGIC_LENGTH = 1;

    // attributes of the record set
    int ATTRIBUTES_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH;
    int ATTRIBUTE_LENGTH = 1;

    // TODO: producer id of the record set
    int PID_OFFSET = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
    int PID_LENGTH = 8;

    // producer epoch number of the record set
    int EPOCH_OFFSET = PID_OFFSET + PID_LENGTH;
    int EPOCH_LENGTH = 4;

    // producer sequence number of the record set
    int SEQUENCE_NUMBER_OFFSET = EPOCH_OFFSET + EPOCH_LENGTH;
    int SEQUENCE_NUMBER_LENGTH = 8;

    /**
     * The size for the record set header
     */
    int HEADER_SIZE_V0 = OFFSET_LENGTH + SIZE_LENGTH;

    int HEADER_SIZE_V1 = OFFSET_LENGTH + SIZE_LENGTH;

    int HEADER_SIZE_V2 = OFFSET_LENGTH + SIZE_LENGTH +
            CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTE_LENGTH +
            PID_LENGTH + EPOCH_LENGTH + SEQUENCE_NUMBER_LENGTH;

    /**
     * The "magic" values, note that for magic value 0 and 1 this class should be used for wrapping a single record
     */
    byte MAGIC_VALUE_V0 = 0;
    byte MAGIC_VALUE_V1 = 1;
    byte MAGIC_VALUE_V2 = 2;

    /**
     * The current "magic" value
     */
    byte CURRENT_MAGIC_VALUE = MAGIC_VALUE_V2;

    /**
     * Specifies the mask for the compression code. 3 bits to hold the compression codec. 0 is reserved to indicate no
     * compression
     */
    int COMPRESSION_CODEC_MASK = 0x07;

    /**
     * The size of these records in bytes
     */
    int sizeInBytes();

}
