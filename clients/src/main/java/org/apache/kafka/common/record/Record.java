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

import org.apache.kafka.common.utils.Crc32;

import java.nio.ByteBuffer;


/**
 * A record: a serialized key and value along with the associated CRC and other fields
 */
public final class Record {

    /**
     * The current offset and size for all the fixed-length fields
     */

    // timestamp of the record
    public static final int TIMESTAMP_OFFSET = 0;
    public static final int TIMESTAMP_LENGTH = 8;

    // serialized key of the record
    public static final int KEY_SIZE_OFFSET = TIMESTAMP_OFFSET + TIMESTAMP_LENGTH;
    public static final int KEY_SIZE_LENGTH = 4;

    public static final int KEY_OFFSET = KEY_SIZE_OFFSET + KEY_SIZE_LENGTH;

    // serialized value of the record
    public static final int VALUE_SIZE_LENGTH = 4;

    /**
     * The amount of overhead bytes in a record
     */
    public static final int RECORD_OVERHEAD = TIMESTAMP_LENGTH + KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH;

    /**
     * Specify the mask of timestamp type.
     * 0 for {@link TimestampType.CREATE_TIME}, 1 for {@link TimestampType.LOG_APPEND_TIME}.
     */
    public static final byte TIMESTAMP_TYPE_MASK = 0x08;
    public static final int TIMESTAMP_TYPE_ATTRIBUTE_OFFSET = 3;

    /**
     * Compression code for uncompressed records
     */
    public static final int NO_COMPRESSION = 0;

    /**
     * Timestamp value for records without a timestamp
     */
    public static final long NO_TIMESTAMP = -1L;

    private final ByteBuffer buffer;

    public Record(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    /**
     * A constructor to create a record by always writting the value payload as is and will not do the compression.
     *
     * @param timestamp The timestamp of the record
     * @param key The key of the record (null, if none)
     * @param value The record value
     * @param valueOffset The offset into the payload array used to extract payload
     * @param valueSize The size of the payload to use
     */
    public Record(long timestamp, byte[] key, byte[] value, int valueOffset, int valueSize) {
        this(ByteBuffer.allocate(recordSize(key == null ? 0 : key.length,
            value == null ? 0 : valueSize >= 0 ? valueSize : value.length - valueOffset)));
        write(this.buffer, timestamp, key, value, valueOffset, valueSize);
        this.buffer.rewind();
    }

    public Record(long timestamp, byte[] key, byte[] value) {
        this(timestamp, key, value, 0, -1);
    }

    public Record(long timestamp, byte[] value) {
        this(timestamp, null, value);
    }

    // Write a record to the buffer
    public static void write(ByteBuffer buffer, long timestamp, byte[] key, byte[] value, int valueOffset, int valueSize) {
        // construct the compressor with compression type none since this function will not do any
        // compression but just write the record's payload as is
        Compressor compressor = new Compressor(buffer, CompressionType.NONE);
        try {
            compressor.putRecord(timestamp, key, value, valueOffset, valueSize);
        } finally {
            compressor.close();
        }
    }

    public static void write(Compressor compressor, long timestamp, byte[] key, byte[] value, int valueOffset, int valueSize) {
        // write timestamp
        compressor.putLong(timestamp);
        // write the key
        if (key == null) {
            compressor.putInt(-1);
        } else {
            compressor.putInt(key.length);
            compressor.put(key, 0, key.length);
        }
        // write the value
        if (value == null) {
            compressor.putInt(-1);
        } else {
            int size = valueSize >= 0 ? valueSize : (value.length - valueOffset);
            compressor.putInt(size);
            compressor.put(value, valueOffset, size);
        }
    }

    /**
     * Compute the checksum of the record from the attributes, key and value payloads
     */
    public static long computeChecksum(long timestamp, byte[] key, byte[] value, int valueOffset, int valueSize) {
        Crc32 crc = new Crc32();

        // update for the timestamp
        crc.updateLong(timestamp);

        // update for the key
        if (key == null) {
            crc.updateInt(-1);
        } else {
            crc.updateInt(key.length);
            crc.update(key, 0, key.length);
        }

        // update for the value
        if (value == null) {
            crc.updateInt(-1);
        } else {
            int size = valueSize >= 0 ? valueSize : (value.length - valueOffset);
            crc.updateInt(size);
            crc.update(value, valueOffset, size);
        }

        return crc.getValue();
    }

    public static int recordSize(byte[] key, byte[] value) {
        return recordSize(key == null ? 0 : key.length, value == null ? 0 : value.length);
    }

    public static int recordSize(int keySize, int valueSize) {
        return TIMESTAMP_LENGTH + KEY_SIZE_LENGTH + keySize + VALUE_SIZE_LENGTH + valueSize;
    }

    public ByteBuffer buffer() {
        return this.buffer;
    }

    /**
     * The complete serialized size of this record in bytes (including crc, header attributes, etc)
     */
    public int size() {
        return buffer.limit();
    }

    /**
     * The length of the key in bytes
     */
    public int keySize() {
        return buffer.getInt(KEY_SIZE_OFFSET);
    }

    /**
     * Does the record have a key?
     */
    public boolean hasKey() {
        return keySize() >= 0;
    }

    /**
     * The position where the value size is stored
     */
    private int valueSizeOffset() {
        return KEY_OFFSET + Math.max(0, keySize());
    }

    /**
     * The length of the value in bytes
     */
    public int valueSize() {
        return buffer.getInt(valueSizeOffset());
    }

    /**
     * Timestamp of the record
     */
    public long timestamp() {
        return buffer.getLong(TIMESTAMP_OFFSET);
    }

    /**
     * A ByteBuffer containing the message key
     */
    public ByteBuffer key() {
        return sliceDelimited(KEY_SIZE_OFFSET);
    }

    /**
     * A ByteBuffer containing the value of this record
     */
    public ByteBuffer value() {
        return sliceDelimited(valueSizeOffset());
    }

    /**
     * Read a size-delimited byte buffer starting at the given offset
     */
    private ByteBuffer sliceDelimited(int start) {
        int size = buffer.getInt(start);
        if (size < 0) {
            return null;
        } else {
            ByteBuffer b = buffer.duplicate();
            b.position(start + 4);
            b = b.slice();
            b.limit(size);
            b.rewind();
            return b;
        }
    }

    public String toString() {
        return String.format("Record(timestamp = %d, key = %d bytes, value = %d bytes)",
                timestamp(),
                key() == null ? 0 : key().limit(),
                value() == null ? 0 : value().limit());
    }

    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (other == null)
            return false;
        if (!other.getClass().equals(Record.class))
            return false;
        Record record = (Record) other;
        return this.buffer.equals(record.buffer);
    }

    public int hashCode() {
        return buffer.hashCode();
    }

}
