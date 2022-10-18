package com.huawei.metadata;

import com.huawei.action.Insert;
import com.huawei.action.Select;
import com.huawei.index.HashIndex;
import com.huawei.storage.DataItem;
import com.huawei.storage.DataManager;

import com.alibaba.fastjson.JSONObject;
import com.google.common.primitives.Bytes;

import cn.hutool.core.date.StopWatch;
import cn.hutool.core.util.ByteUtil;
import lombok.Data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Data
public class Table {
    private String tableName;

    private LinkedList<Field> fields;

    private DataManager dataManager;

    private HashIndex hashIndex = new HashIndex();

    public Table(String tableName, LinkedList<Field> fields) {
        this.tableName = tableName;
        this.fields = fields;
        dataManager = new DataManager(tableName);
    }

    public long insert(Insert insert) throws Exception {
        Map<String, Object> entry = string2Entry(insert.values);
        byte[] raw = entry2Raw(entry);
        long offset = dataManager.insert(raw);
        hashIndex.getIndex().put((long) entry.get("id"), offset);
        return offset;
    }

    private byte[] entry2Raw(Map<String, Object> entry) {
        byte[] raw = new byte[0];

        for (Field field : fields) {
            switch (field.getFieldType()) {
                case "int32":
                    raw = Bytes.concat(raw, ByteUtil.intToBytes((int) entry.get(field.getFieldName())));
                    break;
                case "int64":
                    raw = Bytes.concat(raw, ByteUtil.longToBytes((long) entry.get(field.getFieldName())));
                    break;
                case "string":
                    String str = (String) entry.get(field.getFieldName());
                    byte[] bytes = str.getBytes();
                    raw = Bytes.concat(raw, ByteUtil.shortToBytes((short) str.length()),bytes);
                    break;
            }
        }
        return raw;
    }

    public LinkedList<Map<String, Object>> select(Select select) throws Exception {
        // 判断是否走索引
        LinkedList<Map<String, Object>> maps = new LinkedList<>();
        if(!select.whereMap.isEmpty()){
            // 走索引
            long id = select.getWhereMap().get("id").get(1);
            Long offset = hashIndex.getIndex().get(id);
            if(offset!=null){
                DataItem dataItem = dataManager.read(offset);
                Map<String, Object> entry = parseEntry(dataItem.getBuffer(), select);
                maps.add(entry);
            }
        }else{
            // 走全盘扫描
            dataManager.position(0);
            DataItem dataItem;
            while ((dataItem = dataManager.next()) != null) {
                Map<String, Object> entry = parseEntry(dataItem.getBuffer(), select);
                if (entry != null) {
                    maps.add(entry);
                }
            }
        }
        return maps;

    }

    // 处理一行数据
    private Map<String, Object> parseEntry(ByteBuffer raw, Select select) {
        Map<String, Object> entry = new HashMap<>();
        Object result = null;
        for (Field field : fields) {
            switch (field.getFieldType()) {
                case "int32":
                    result = raw.getInt();
                    break;
                case "int64":
                    result = raw.getLong();
                    break;
                case "string":
                    byte[] bytes = new byte[raw.getShort()];
                    raw.get(bytes);
                    result = new String(bytes);
                    break;
            }
            if (!select.match(field, result)) {
                return null;
            }
            if (select.isSelectStar() || select.getFieldSet().contains(field.getFieldName())) {
                entry.put(field.getFieldName(), result);
            }
        }
        return entry;
    }

    private Map<String, Object> string2Entry(String[] values) throws Exception {
        if (values.length != fields.size()) {
            throw new RuntimeException("Invalid values!");
        }
        Map<String, Object> entry = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            Object v = f.string2Value(values[i]);
            entry.put(f.getFieldName(), v);
        }
        return entry;
    }

    private byte[] string2Raw(String[] values) {
        if (values.length != fields.size()) {
            throw new RuntimeException("Invalid values!");
        }
        byte[] raw = new byte[0];
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            switch (f.getFieldType()) {
                case "int32":
                    raw = Bytes.concat(raw, ByteUtil.intToBytes(Integer.parseInt(values[i])));
                    break;
                case "int64":
                    raw = Bytes.concat(raw, ByteUtil.longToBytes(Long.parseLong(values[i])));
                    break;
                case "string":
                    byte[] bytes = values[i].getBytes();
                    raw = Bytes.concat(raw, ByteUtil.shortToBytes((short) values[i].length()));
                    raw = Bytes.concat(raw, bytes);
                    break;
            }
        }
        return raw;
    }

    public void close() throws IOException {
        dataManager.close();
    }
}
