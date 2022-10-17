package com.huawei;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.file.FileMode;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws IOException {
        // 测试 创建表
        // 启动创建元数据文件

        Executor executor = new Executor();
        executor.createTable("testTable");
        executor.insertData("testTable",new People("bbi",12));
        System.out.println("Hello World!");
    }
}
