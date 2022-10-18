package com.huawei.metadata;

import com.huawei.action.Insert;
import com.huawei.action.Select;
import com.huawei.index.HashIndex;
import com.huawei.storage.DataItem;
import com.huawei.storage.DataManager;

import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;

import cn.hutool.core.lang.Tuple;
import cn.hutool.core.util.ByteUtil;
import lombok.Data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
public class Table {
    private String tableName;

    private Long rowNum;

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
                    raw = Bytes.concat(raw, ByteUtil.shortToBytes((short) str.length()), bytes);
                    break;
            }
        }
        return raw;
    }

    public LinkedList<Map<String, Object>> select(Select select) throws Exception {
        // 判断是否走索引
        LinkedList<Map<String, Object>> maps = new LinkedList<>();
        // 生成执行计划
        //
        Explain explain = generateExplain(select);
        return handleByExplain(explain,select);
    }

    private LinkedList<Map<String, Object>> handleByExplain(Explain explain,Select select) throws Exception {
        LinkedList<Map<String, Object>> maps = new LinkedList<>();
        if(explain.getType().equals("system")){
            DataItem dataItem = dataManager.read(0);
            Map<String, Object> entry = parseEntry(dataItem.getBuffer(), select);
            if (entry != null) {
              maps.add(entry);
            }
        }else if(explain.getType().equals("ALL")){
            dataManager.position(0);
            DataItem dataItem;
            while ((dataItem = dataManager.next()) != null) {
                Map<String, Object> entry = parseEntry(dataItem.getBuffer(), select);
                if (entry != null) {
                    maps.add(entry);
                }
            }
        }else{
            // 走索引
            long id = select.getWhereMap().get(explain.getKey()).get(1);
            Long offset = hashIndex.getIndex().get(id);
            if (offset != null) {
                DataItem dataItem = dataManager.read(offset);
                Map<String, Object> entry = parseEntry(dataItem.getBuffer(), select);
                maps.add(entry);
            }
        }
        return maps;
    }

    private Explain generateExplain(Select select) {
        Explain explain = new Explain();
        if (rowNum == 1) {
            explain.setType("system");
            return explain;
        }
        List<Tuple> where = select.where;
        LinkedList<Field> fields = this.fields;
        List<Tuple> collect = where.stream().filter(tuple -> {
            for (Field field1 : fields) {
                if (field1.getFieldName().equals(tuple.get(0).toString())) {
                    return field1.isIndexed();
                }
            }
            return false;
        }).collect(Collectors.toList());
        if (collect.isEmpty()) {
            // 没有找到索引
            explain.setType("ALL");
        } else {
            // 暂时直接选第一个
            Tuple objects = collect.get(0);
            explain.setPossible_keys(Lists.transform(collect,tuple->tuple.get(0)).toString());
            explain.setKey(objects.get(0));
            explain.setType("const");
        }
        return explain;
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
