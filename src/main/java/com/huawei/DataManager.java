package com.huawei.storage;

import com.google.common.primitives.Bytes;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.file.FileMode;
import cn.hutool.core.util.ByteUtil;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

/**
 * 提供方法存储数据 返回数据的offset
 */
public class DataManager {

    // 持有一个文件
    FileChannel channel;

    public DataManager(String tableName) {
        RandomAccessFile randomAccessFile = FileUtil.createRandomAccessFile(FileUtil.file(tableName + ".tb"),
            FileMode.rw);
        channel = randomAccessFile.getChannel();
    }

    public DataItem read(long offset) throws Exception {
        channel.position(offset);
        return next();
    }

    public long insert(byte[] data) throws Exception {
        long size = channel.size();
        byte[] concat = Bytes.concat(ByteUtil.shortToBytes((short) data.length), data);
        channel.write(ByteBuffer.wrap(concat), size);

        return size;
    }

    public void close() throws IOException {
        channel.close();
    }

    public void position(int i) throws IOException {
        channel.position(i);
    }

    public DataItem next() throws IOException {
        if(channel.size() == channel.position())return null;
        ByteBuffer buf = ByteBuffer.allocate(2);
        channel.read(buf);
        short size = ByteUtil.bytesToShort(buf.array());
        ByteBuffer data = ByteBuffer.allocate(size);
        data.order(ByteOrder.LITTLE_ENDIAN);
        channel.read(data);
        data.rewind();
        return new DataItem(size,data);
    }
}
