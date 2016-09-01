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
     * The current size for all the fixed-length fields
     */

    // crc of the record set
    public static final int CRC_LENGTH = 4;

    // serialized value of the record
    public static final int VALUE_SIZE_LENGTH = 4;

    /**
     * Timestamp value for records without a timestamp
     */
    public static final long NO_TIMESTAMP = -1L;

    public static int recordSize(long timestampDelta, int offsetDelta, byte[] key, byte[] value) {
        return recordSize(timestampDelta, offsetDelta, key == null ? 0 : key.length, value == null ? 0 : value.length);
    }

    public static int recordSize(long timestampDelta, int offsetDelta, int keySize, int valueSize) {
        return Utils.bytesForVarLongEncoding(timestampDelta)
                + Utils.bytesForUnsignedVarIntEncoding(offsetDelta)
                + (keySize == -1 ? Utils.bytesForVarIntEncoding(-1) : Utils.bytesForVarIntEncoding(keySize) + keySize)
                + VALUE_SIZE_LENGTH + valueSize
                + CRC_LENGTH;
    }

    private final byte[] key;

    private final byte[] value;

    private final long timestampDelta;

    private final int offsetDelta;

    /**
     * A constructor to create a record.
     *
     * @param timestampDelta The timestamp of the record
     * @param offsetDelta Delta offset of the record compared to its belonged record set base offset
     * @param key The key of the record (null, if none)
     * @param value The record value
     */
    public Record(long timestampDelta, int offsetDelta, byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
        this.timestampDelta = timestampDelta;
        this.offsetDelta = offsetDelta;
    }

    public Record(long timestampDelta, int offsetDelta, byte[] value) {
        this(timestampDelta, offsetDelta, null, value);
    }

    /**
     * Write a record into the specified {@link Compressor} and return the checksum
     */
    public static long write(Compressor compressor, long timestampDelta, int offsetDelta, byte[] key, byte[] value) {
        // compute the checksum
        long crc = Record.computeChecksum(timestampDelta, offsetDelta, key, value);

        write(compressor, timestampDelta, offsetDelta, key, value, crc);

        return crc;
    }

    /**
     * Write a record into the specified {@link Compressor} along with the expected checksum,
     * this is used for unit test only
     */
    public static long write(Compressor compressor, long timestampDelta, int offsetDelta, byte[] key, byte[] value, long crc) {
        // write delta timestamp
        compressor.put(Utils.writeVarLong(timestampDelta));
        // write delta offset
        compressor.put(Utils.writeUnsignedVarInt(offsetDelta));
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

        // write the checksum
        compressor.putInt((int) (crc & 0xffffffffL));

        return crc;
    }

    /**
     * Read a record out of the {@link DataInput}
     */
    public static Record read(DataInput in) throws IOException {
        // read delta timestamp
        long timestampDelta = Utils.readVarLong(in);

        // read the delta offset with variable-length encoding
        int offsetDelta = Utils.readUnsignedVarInt(in);

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

        return new Record(timestampDelta, offsetDelta, key, value);
    }

    /**
     * Compute the CRC (checksum) of the record from the timestamp delta, offset delta, key and value payloads
     */
    public static long computeChecksum(long timestampDelta, int offsetDelta, byte[] key, byte[] value) {
        Crc32 crc = new Crc32();

        // update for the delta timestamp
        crc.updateLong(timestampDelta);

        // update for the delta offset
        crc.updateInt(offsetDelta);

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
            crc.updateInt(value.length);
            crc.update(value, 0, value.length);
        }

        return crc.getValue();
    }

    /**
     * The checksum of this record based on its fields
     */
    public long checksum() {
        return computeChecksum(timestampDelta, offsetDelta, key, value);
    }

    /**
     * Returns true if the crc stored with the record matches the crc computed off the record contents
     */
    public boolean isValid() {
        return checksum() == computeChecksum();
    }

    /**
     * Throw an InvalidRecordException if isValid is false for this record
     */
    public void ensureValid() {
        if (!isValid())
            throw new InvalidRecordException("Record is corrupt (stored crc = " + checksum()
                    + ", computed crc = "
                    + computeChecksum()
                    + ")");
    }

    /**
     * Compute the checksum of the record from the record contents
     */
    public long computeChecksum() {
        return computeChecksum(this.timestampDelta, this.offsetDelta, this.key, this.value);
    }

    /**
     * The complete serialized size of this record in bytes
     */
    public int size() {
        return Record.recordSize(timestampDelta, offsetDelta, key, value);
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
        return timestampDelta;
    }

    /**
     * Offset of the record
     */
    public int offset() {
        return offsetDelta;
    }

    /**
     * Delta offset of the record within its belonging record set
     */
    public int deltaOffset() {
        return offsetDelta;
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
        return String.format("Record(timestamp delta = %d, offset delta = %d, key = %d bytes, value = %d bytes)",
                timestamp(),
                offset(),
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

        return this.timestampDelta == record.timestamp() &&
                this.offsetDelta == record.deltaOffset() &&
                comparator.compare(this.key, record.key()) == 0 &&
                comparator.compare(this.value, record.value()) ==0;
    }

    public int hashCode() {
        long result = (timestampDelta << 32) | offsetDelta;
        result = (result << 32) | Arrays.hashCode(key);
        result = (result << 32) | Arrays.hashCode(value);
        return (int) (result % 0xFFFFFFFFL);
    }
}
