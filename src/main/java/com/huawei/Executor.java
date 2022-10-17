package com.huawei;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.file.FileMode;
import com.alibaba.fastjson.JSONObject;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;

public class Executor {
    RandomAccessFile randomAccessFile ;
    File metadata;
    public Executor() throws IOException {
        metadata = FileUtil.file(".metadata");
        metadata.createNewFile();
    }


    public void createTable(String tableName) throws IOException {
        FileUtil.appendString(tableName,metadata, Charset.defaultCharset());
        File file = FileUtil.file(tableName + ".tb");
        file.createNewFile();
    }

    public void insertData(String tableName,Object obj){
        File file = FileUtil.file(tableName + ".tb");
        FileUtil.appendString(JSONObject.toJSONString(obj),file, Charset.defaultCharset());
    }
}
