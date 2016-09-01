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
    int ATTRIBUTE_LENGTH = 4;

    // producer context of the record set
    int PRODUCER_CONTEXT_OFFSET = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
    int PRODUCER_CONTEXT_LENGTH = ProducerContext.TOTAL_LENGTH;

    // base timestamp of the record set
    int TIMESTAMP_OFFSET = PRODUCER_CONTEXT_OFFSET + PRODUCER_CONTEXT_LENGTH;
    int TIMESTAMP_LENGTH = 8;

    // number of records in this record set
    int NUM_RECORDS_OFFSET = TIMESTAMP_OFFSET + TIMESTAMP_LENGTH;
    int NUM_RECORDS_LENGTH = 4;

    /**
     * The size for the record set header
     */
    int RECORDS_HEADER_SIZE_V0 = SIZE_OFFSET + SIZE_LENGTH;

    int RECORDS_HEADER_SIZE_V1 = SIZE_OFFSET + SIZE_LENGTH;

    int RECORDS_HEADER_SIZE_V2 = NUM_RECORDS_OFFSET + NUM_RECORDS_LENGTH;

    int RECORDS_OVERHEAD = OFFSET_LENGTH + SIZE_LENGTH;

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
    long COMPRESSION_CODEC_MASK = 0x00000007L;

    /**
     * Compression code for uncompressed records
     */
    int NO_COMPRESSION = 0;

    /**
     * Specify the mask of timestamp type.
     * 0 for {@link TimestampType.CREATE_TIME}, 1 for {@link TimestampType.LOG_APPEND_TIME}.
     */
    long TIMESTAMP_TYPE_MASK = 0x00000008L;
    int TIMESTAMP_TYPE_ATTRIBUTE_OFFSET = 3;

    /**
     * The size of these records in bytes
     */
    int sizeInBytes();
}
