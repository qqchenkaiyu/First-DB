package com.huawei;

import static org.junit.Assert.assertTrue;

import com.huawei.action.Insert;
import com.huawei.action.Select;
import com.huawei.metadata.Field;
import com.huawei.metadata.Table;

import com.alibaba.fastjson.JSON;

import cn.hutool.core.date.StopWatch;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.lang.Snowflake;
import cn.hutool.core.lang.Tuple;
import cn.hutool.core.util.IdUtil;

import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void testPerformance() throws Exception {
        // 测试 创建表
        // 启动创建元数据文件
        LinkedList<Field> fields = new LinkedList<>();
        fields.add(new Field("id", "int64",true));
        fields.add(new Field("name", "string",false));
        fields.add(new Field("age", "int32",false));
        Table testTable = new Table("testTable", fields);
        // executor.createTable(testTable);
        Snowflake snowflake = IdUtil.getSnowflake();
        int test = 50 * 10000;
        StopWatch stopWatch = new StopWatch("性能测试");
        stopWatch.start("插入"+test+"数据");
        for (int i = 0; i < test; i++) {
            Insert insert = new Insert(testTable.getTableName(), new String[] {snowflake.nextIdStr(),"bbi", "12"});
            if (i % 10000 == 0) {
                insert = new Insert(testTable.getTableName(), new String[] {String.valueOf(i),"lilian", "12"});
            }
            testTable.insert(insert);
        }
        stopWatch.stop();
        List<Tuple> tuples = Arrays.asList(new Tuple("id", 10000L));
        Select select = new Select(new String[] {"name"}, tuples);
        stopWatch.start("全盘查询"+test+"数据");
        LinkedList<Map<String, Object>> res = testTable.select(select);
        stopWatch.stop();
        System.out.println(stopWatch.prettyPrint(TimeUnit.MILLISECONDS));

        System.out.println(JSON.toJSONString(res));
        // 结束测试
        testTable.close();
        FileUtil.del("testTable.tb");
    }
}
