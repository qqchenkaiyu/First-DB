package com.huawei.index;

import lombok.Data;

import java.util.HashMap;
@Data
public class HashIndex {
    // id->offset
    HashMap<Long,Long> index = new HashMap();
}
