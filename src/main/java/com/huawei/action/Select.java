package com.huawei.action;

import com.huawei.metadata.Field;

import cn.hutool.core.lang.Tuple;
import lombok.Data;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Data
public class Select {
    public String tableName;

    public String[] fields;

    public List<Tuple> where;

    public HashMap<String, Tuple> whereMap;

    public boolean selectStar = true;

    public Set<String> fieldSet;

    public Select() {

    }

    public boolean match(Map<String, Object> entry) {
        for (Tuple tuple : where) {
            Object o = entry.get(tuple.get(0));
            if (!o.equals(tuple.get(1))) {
                return false;
            }
        }
        return true;
    }

    public Select(String[] fields, List<Tuple> where) {
        this.fields = fields;
        if (fields != null && fields.length != 0) {
            selectStar = false;
            fieldSet = Arrays.stream(fields).collect(Collectors.toSet());
        }
        this.where = where;
        this.whereMap = new HashMap<>();
        for (Tuple objects : where) {
            whereMap.put(objects.get(0), objects);
        }

    }

    public boolean match(Field field, Object result) {
        Tuple tuple = whereMap.get(field.getFieldName());
        if (tuple == null) {
            // 说明不是查询条件
            return true;
        }
        return tuple.get(1).equals(result);
    }
}
