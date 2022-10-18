package com.huawei.storage;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
public class DataItem {
    private short size;

    private ByteBuffer buffer;

    public DataItem(short size, ByteBuffer buffer) {
        this.size = size;
        this.buffer = buffer;
    }
}
