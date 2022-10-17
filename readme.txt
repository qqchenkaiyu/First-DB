1.简单实现数据的插入和全盘加载
格式定义
tb.metadata 每行都是一个表的元数据说明，有多少行就说明有多少表  结构{"tableName":"",fields:[{"fieldName":"age","fieldType":int}]}
stu.tb  每行都是stu表的一行数据，每行数据有个字段表示是否有效，用于删除 结构 {"name","lili","age":18}

create表操作 --- 在metadata文件进行插入 创建空tb文件
insert操作 -- 往空tb文件插入数据

遍历操作： 遍历tb文件，打印数据