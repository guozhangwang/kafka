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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Crc32;
import org.apache.kafka.common.utils.Utils;

import java.io.DataInput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;


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

    // serialized value of the record
    public static final int VALUE_SIZE_LENGTH = 4;

    /**
     * Timestamp value for records without a timestamp
     */
    public static final long NO_TIMESTAMP = -1L;

    public static int recordSize(int deltaOffset, byte[] key, byte[] value) {
        return recordSize(key == null ? 0 : key.length, value == null ? 0 : value.length, deltaOffset);
    }

    public static int recordSize(int deltaOffset, int keySize, int valueSize) {
        return TIMESTAMP_LENGTH + Utils.bytesForUnsignedVarIntEncoding(deltaOffset)
                + Utils.bytesForUnsignedVarIntEncoding(keySize) + keySize
                + VALUE_SIZE_LENGTH + valueSize;
    }

    private final byte[] key;

    private final byte[] value;

    private final long timestamp;

    private final int deltaOffset;

    /**
     * A constructor to create a record.
     *
     * @param timestamp The timestamp of the record
     * @param deltaOffset Delta offset of the record compared to its belonged record set base offset
     * @param key The key of the record (null, if none)
     * @param value The record value
     */
    public Record(long timestamp, int deltaOffset, byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.deltaOffset = deltaOffset;
    }

    public Record(long timestamp, int deltaOffset, byte[] value) {
        this(timestamp, deltaOffset, null, value);
    }

    /**
     * Write a record into the specified {@link Compressor}
     */
    public static void write(Compressor compressor, long timestamp, int deltaOffset, byte[] key, byte[] value) {
        // write timestamp
        compressor.putLong(timestamp);
        // write delta offset
        compressor.put(Utils.writeUnsignedVarInt(deltaOffset));
        // write the key
        if (key == null) {
            compressor.put(Utils.writeVarInt(-1));
        } else {
            int size = key.length;
            compressor.put(Utils.writeUnsignedVarInt(size));
            compressor.put(key, 0, size);
        }
        // write the value
        if (value == null) {
            compressor.putInt(-1);
        } else {
            int size = value.length;
            compressor.putInt(size);
            compressor.put(value, 0, size);
        }
    }

    /**
     * Read a record out of the {@link DataInput}
     */
    public static Record read(DataInput in) throws IOException {
        // read timestamp
        long timestamp = in.readLong();

        // read the delta offset with variable-length encoding
        int deltaOffset = Utils.readUnsignedVarInt(in);

        // read the key, by reading the key size with variable-length encoding first
        int keySize = Utils.readVarInt(in);
        byte[] key = keySize < 0 ? null : new byte[keySize];
        if (key != null)
            in.readFully(key);

        // read the value, by reading the value size first
        int valueSize = in.readInt();
        byte[] value = valueSize < 0 ? null : new byte[valueSize];
        if (value != null)
            in.readFully(value);

        return new Record(timestamp, deltaOffset, key, value);
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

    /**
     * The complete serialized size of this record in bytes
     */
    public int size() {
        return TIMESTAMP_LENGTH + Utils.bytesForUnsignedVarIntEncoding(deltaOffset)
                + (key == null ? Utils.bytesForVarIntEncoding(-1) : Utils.bytesForVarIntEncoding(key.length) + key.length)
                + VALUE_SIZE_LENGTH + value.length;
    }

    /**
     * The length of the key in bytes
     */
    public int keySize() {
        return key.length;
    }

    /**
     * Does the record have a key?
     */
    public boolean hasKey() {
        return key != null;
    }

    /**
     * The length of the value in bytes
     */
    public int valueSize() {
        return value.length;
    }

    /**
     * Timestamp of the record
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * Delta offset of the record within its belonging record set
     */
    public int deltaOffset() {
        return deltaOffset;
    }

    /**
     * A ByteBuffer containing the message key
     */
    public byte[] key() {
        return key;
    }

    /**
     * A ByteBuffer containing the value of this record
     */
    public byte[] value() {
        return value;
    }

    public String toString() {
        return String.format("Record(timestamp = %d, key = %d bytes, value = %d bytes)",
                timestamp(),
                key() == null ? 0 : keySize(),
                value() == null ? 0 : valueSize());
    }

    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (other == null)
            return false;
        if (!other.getClass().equals(Record.class))
            return false;
        Record record = (Record) other;

        Comparator<byte[]> comparator = Bytes.BYTES_LEXICO_COMPARATOR;

        return this.timestamp == record.timestamp() &&
                this.deltaOffset == record.deltaOffset() &&
                comparator.compare(this.key, record.key()) == 0 &&
                comparator.compare(this.value, record.value()) ==0;
    }

    public int hashCode() {
        long result = (timestamp << 16) | deltaOffset;
        result = (result << 32) | Arrays.hashCode(key);
        result = (result << 32) | Arrays.hashCode(value);
        return (int) (result % 0xFFFFFFFFL);
    }
}
