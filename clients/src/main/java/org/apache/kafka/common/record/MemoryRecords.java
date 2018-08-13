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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Iterator;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.AbstractIterator;

/**
 * A {@link Records} implementation backed by a ByteBuffer.
 */
public class MemoryRecords implements Records { //只能通过 emptyRecords(...) 构造 指定 压缩 一些列的 put...(...)

    private final static int WRITE_LIMIT_FOR_READABLE_ONLY = -1;

    // the compressor used for appends-only
    private final Compressor compressor; // 压缩器 压缩完成后 传输入buffer

    // the write limit for writable buffer, which may be smaller than the buffer capacity
    private final int writeLimit; //最多写入

    // the capacity of the initial buffer, which is only used for de-allocation of writable records
    private final int initialCapacity;

    // the underlying buffer used for read; while the records are still writable it is null
    private ByteBuffer buffer; // java nio ByteBuffer 封装了 java nio 的 ByteBuffer

    // indicate if the memory records is writable or not (i.e. used for appends or read-only)
    private boolean writable;  // 是否 可读可写

    // Construct a writable memory records
    private MemoryRecords(ByteBuffer buffer, CompressionType type, boolean writable, int writeLimit) {
        this.writable = writable;
        this.writeLimit = writeLimit;
        this.initialCapacity = buffer.capacity();
        if (this.writable) {
            this.buffer = null;
            this.compressor = new Compressor(buffer, type);
        } else {
            this.buffer = buffer;
            this.compressor = null;
        }
    }

    public static MemoryRecords emptyRecords(ByteBuffer buffer, CompressionType type, int writeLimit) {
        return new MemoryRecords(buffer, type, true, writeLimit);
    }

    public static MemoryRecords emptyRecords(ByteBuffer buffer, CompressionType type) {
        // use the buffer capacity as the default write limit
        return emptyRecords(buffer, type, buffer.capacity());
    }

    public static MemoryRecords readableRecords(ByteBuffer buffer) {
        return new MemoryRecords(buffer, CompressionType.NONE, false, WRITE_LIMIT_FOR_READABLE_ONLY);
    }

    /**
     * Append the given record and offset to the buffer
     */
    public void append(long offset, Record record) {
        if (!writable)  // j检查是否可以写
            throw new IllegalStateException("Memory records is not writable");
        // 消息 offset size message
        int size = record.size();
        compressor.putLong(offset);
        compressor.putInt(size);
        compressor.put(record.buffer());
        compressor.recordWritten(size + Records.LOG_OVERHEAD);
        record.buffer().rewind();
    }

    /**
     * Append a new record and offset to the buffer
     * @return crc of the record
     */
    public long append(long offset, long timestamp, byte[] key, byte[] value) {
        if (!writable)
            throw new IllegalStateException("Memory records is not writable");

        int size = Record.recordSize(key, value);
        compressor.putLong(offset); // 8个字节
        compressor.putInt(size);// 4个字节
        long crc = compressor.putRecord(timestamp, key, value);
        compressor.recordWritten(size + Records.LOG_OVERHEAD); // 头信息 占用大小 consumer 时 使用 截取4个字节的头信息
        return crc;
    }

    /**
     * Check if we have room for a new record containing the given key/value pair
     *
     * Note that the return value is based on the estimate of the bytes written to the compressor, which may not be
     * accurate if compression is really used. When this happens, the following append may cause dynamic buffer
     * re-allocation in the underlying byte buffer stream.
     *
     * There is an exceptional case when appending a single message whose size is larger than the batch size, the
     * capacity will be the message size which is larger than the write limit, i.e. the batch size. In this case
     * the checking should be based on the capacity of the initialized buffer rather than the write limit in order
     * to accept this single record.
     */
    public boolean hasRoomFor(byte[] key, byte[] value) {
        if (!this.writable) // 是否 关闭写入
            return false;

        return this.compressor.numRecordsWritten() == 0 ?
            this.initialCapacity >= Records.LOG_OVERHEAD + Record.recordSize(key, value) :
            this.writeLimit >= this.compressor.estimatedBytesWritten() + Records.LOG_OVERHEAD + Record.recordSize(key, value); // 只是一个预估值 后面涉及到buffer扩容
    }

    public boolean isFull() {
        return !this.writable || this.writeLimit <= this.compressor.estimatedBytesWritten();
    }

    /**
     * Close this batch for no more appends
     */
    public void close() {
        if (writable) {
            // close the compressor to fill-in wrapper message metadata if necessary
            compressor.close();

            // flip the underlying buffer to be ready for reads
            buffer = compressor.buffer();
            buffer.flip(); // 刷新 position

            // reset the writable flag
            writable = false;
        }
    }

    /**
     * The size of this record set
     */
    public int sizeInBytes() {
        if (writable) {
            return compressor.buffer().position();
        } else {
            return buffer.limit();
        }
    }

    /**
     * The compression rate of this record set
     */
    public double compressionRate() {
        if (compressor == null)
            return 1.0;
        else
            return compressor.compressionRate();
    }

    /**
     * Return the capacity of the initial buffer, for writable records
     * it may be different from the current buffer's capacity
     */
    public int initialCapacity() {
        return this.initialCapacity;
    }

    /**
     * Get the byte buffer that backs this records instance for reading
     */
    public ByteBuffer buffer() {
        if (writable)
            throw new IllegalStateException("The memory records must not be writable any more before getting its underlying buffer");

        return buffer.duplicate();
    }

    @Override
    public Iterator<LogEntry> iterator() {
        if (writable) {
            // flip on a duplicate buffer for reading
            return new RecordsIterator((ByteBuffer) this.buffer.duplicate().flip(), false);
        } else {
            // do not need to flip for non-writable buffer
            return new RecordsIterator(this.buffer.duplicate(), false);
        }
    }
    
    @Override
    public String toString() {
        Iterator<LogEntry> iter = iterator();
        StringBuilder builder = new StringBuilder();
        builder.append('[');
        while (iter.hasNext()) {
            LogEntry entry = iter.next();
            builder.append('(');
            builder.append("offset=");
            builder.append(entry.offset());
            builder.append(",");
            builder.append("record=");
            builder.append(entry.record());
            builder.append(")");
        }
        builder.append(']');
        return builder.toString();
    }

    /** Visible for testing */
    public boolean isWritable() {
        return writable;
    }

    /**
     *   ----①-------------------------------------
     *   |   offset = 3031   |   offset = 3037     |
     *   |  ②               ⑧                    |
     *   | 0  1  2  3  4  5  |  0  1  2  3  4  5   |
     *  ----③-④-⑤-⑥-⑦-------------------------
     *
     * RecordIterator 压缩解析
     *  首先通过public 构造函数创建MemoryRecords.RecordIterator对象作为浅层迭代器并调用next()
     *  方法，此时字段state 为NOT_READY， 调用makeNext()方法准备迭代项，在makeNext() 方法中会判断
     *  深层迭代是否完成（即 innerDone()方法），当前未开始深层迭代则调用getNextEntryFromStream() 获取offset = 3031
     *  的消息，之后检测到当前消息是压缩格式，假设采取gzip 的压缩模式，则通过private构造函数创建MemoryRecords.RecordsIterator
     *  对象作为深层迭代器，在创建过程中会创建对应的解压缩输出流，然后调用getNextEntryFromStream方法解压offset = 3031的外层消息
     *   其中嵌套的压缩消息形成logEntries队列 然后调用深层迭代器的next()方法，因为不存在第三层迭代且logEntries不为null ，则从logEntries
     *   集合中获取消息并返回，此过程对应步骤②， 后续迭代中深层迭代未完成则继续迭代步骤③-④-⑤-⑥-⑦，迭代完成后调用getNextEntryFromStream()
     *   获取offset = 3037 上述的步骤⑧ 后续迭代与上述过程重复，不再赘述
     *
     *
     *
     */
    public static class RecordsIterator extends AbstractIterator<LogEntry> {
        private final ByteBuffer buffer;  // 指向MemoryRecords 的buffer 其中的消息 可以是压缩的
        private final DataInputStream stream; // 读取buffer的输入流
        private final CompressionType type; // 压缩模式
        private final boolean shallow; // 表示当前的RecordsIterator 是深层迭代器还是浅层迭代器
        private RecordsIterator innerIter; // 迭代压缩消息的 Inner Iterator

        // The variables for inner iterator
        private final ArrayDeque<LogEntry> logEntries;
        private final long absoluteBaseOffset;

        public RecordsIterator(ByteBuffer buffer, boolean shallow) {
            this.type = CompressionType.NONE;
            this.buffer = buffer;
            this.shallow = shallow;
            this.stream = new DataInputStream(new ByteBufferInputStream(buffer));
            this.logEntries = null;
            this.absoluteBaseOffset = -1;
        }

        // Private constructor for inner iterator.
        private RecordsIterator(LogEntry entry) {
            // 压缩模式
            this.type = entry.record().compressionType();
            // buffer数据
            this.buffer = entry.record().value();
            // 是否千层迭代
            this.shallow = true;
            // 获取压缩输入流
            this.stream = Compressor.wrapForInput(new ByteBufferInputStream(this.buffer), type, entry.record().magic());
            // 压缩外部offset
            long wrapperRecordOffset = entry.offset();
            // If relative offset is used, we need to decompress the entire message first to compute
            // the absolute offset.
            if (entry.record().magic() > Record.MAGIC_VALUE_V0) {
                this.logEntries = new ArrayDeque<>();
                long wrapperRecordTimestamp = entry.record().timestamp();
                while (true) {
                    try {
                        // 读取并解压消息
                        LogEntry logEntry = getNextEntryFromStream();
                        Record recordWithTimestamp = new Record(logEntry.record().buffer(),
                                                                wrapperRecordTimestamp,
                                                                entry.record().timestampType());
                        logEntries.add(new LogEntry(logEntry.offset(), recordWithTimestamp));
                    } catch (EOFException e) {
                        break;
                    } catch (IOException e) {
                        throw new KafkaException(e);
                    }
                }
                this.absoluteBaseOffset = wrapperRecordOffset - logEntries.getLast().offset();
            } else {
                this.logEntries = null;
                this.absoluteBaseOffset = -1;
            }

        }

        /*
         * Read the next record from the buffer.
         * 
         * Note that in the compressed message set, each message value size is set as the size of the un-compressed
         * version of the message value, so when we do de-compression allocating an array of the specified size for
         * reading compressed value data is sufficient.
         */
        @Override
        protected LogEntry makeNext() {
            // 判断压缩消息是否消费完
            if (innerDone()) {
                try {
                    LogEntry entry = getNextEntry();
                    // No more record to return.
                    if (entry == null)
                        return allDone();

                    // Convert offset to absolute offset if needed. 在消费的时候 将压缩的消息需要更绝对offset值
                    if (absoluteBaseOffset >= 0) {
                        long absoluteOffset = absoluteBaseOffset + entry.offset();
                        entry = new LogEntry(absoluteOffset, entry.record());
                    }

                    // decide whether to go shallow or deep iteration if it is compressed
                    CompressionType compression = entry.record().compressionType();
                    if (compression == CompressionType.NONE || shallow) {
                        return entry;
                    } else {
                        // init the inner iterator with the value payload of the message,
                        // which will de-compress the payload to a set of messages;
                        // since we assume nested compression is not allowed, the deep iterator
                        // would not try to further decompress underlying messages
                        // There will be at least one element in the inner iterator, so we don't
                        // need to call hasNext() here.
                        // 循环内部的 消息 赋值给 innerIter
                        innerIter = new RecordsIterator(entry);
                        return innerIter.next();
                    }
                } catch (EOFException e) {
                    return allDone();
                } catch (IOException e) {
                    throw new KafkaException(e);
                }
            } else {
                return innerIter.next();
            }
        }

        private LogEntry getNextEntry() throws IOException {
            if (logEntries != null)
                return getNextEntryFromEntryList();
            else
                return getNextEntryFromStream();
        }

        private LogEntry getNextEntryFromEntryList() {
            return logEntries.isEmpty() ? null : logEntries.remove();
        }

        private LogEntry getNextEntryFromStream() throws IOException {
            // read the offset
            long offset = stream.readLong();
            // read record size
            int size = stream.readInt();
            if (size < 0)
                throw new IllegalStateException("Record with size " + size);
            // read the record, if compression is used we cannot depend on size
            // and hence has to do extra copy
            ByteBuffer rec;
            if (type == CompressionType.NONE) {
                rec = buffer.slice();
                int newPos = buffer.position() + size;
                if (newPos > buffer.limit())
                    return null;
                buffer.position(newPos);
                rec.limit(size);
            } else {
                byte[] recordBuffer = new byte[size];
                stream.readFully(recordBuffer, 0, size);
                rec = ByteBuffer.wrap(recordBuffer);
            }
            return new LogEntry(offset, new Record(rec));
        }

        private boolean innerDone() {
            return innerIter == null || !innerIter.hasNext();
        }
    }
}
