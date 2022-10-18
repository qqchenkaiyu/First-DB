package com.huawei.metadata;

import lombok.Data;

@Data
public class Explain {
    // 查询类型 一般为simple简单查询
    private String select_type;
    // 查询用到的表
    private String table;
    // 当表中只有一行 -- system
    // 使用唯一索引 const
    // 非唯一索引 ref
    // 索引范围查询 range
    // 索引全表扫描 index
    // 全表扫描 all
    private String type;
    // select 中可能用到的索引
    private String possible_keys;
    // 最后用到的索引
    private String key;
    // 索引字段的长度
    private String key_len;
    private String ref;
    // 预计扫描的行数
    private String rows;
    private String filtered;
    private String extra;
}
