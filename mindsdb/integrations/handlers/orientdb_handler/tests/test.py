# pyorient 包学习,用于简单入门 python + orientdb
import numpy as np
import pandas as pd
import pyorient
if __name__ == '__main__':
    # 连接服务器
    client = pyorient.OrientDB("192.168.10.128", 2424)
    session_id = client.connect("root", "123456")
    client.db_open("test", "root", "123456")

    # 服务器关闭
    # client.shutdown('root', '123456')

    # This method lists the databases available on the connected OrientDB Server.
    # The __getattr__() method ensures that you retrieve a dict instead of an OrientRecord object.
    # print(client.db_list().__getattr__('databases'))

    # 1.数据库创建 db_create
    # try:
    #     client.db_create(
    #         "tinkerhome",
    #         pyorient.DB_TYPE_GRAPH,
    #         pyorient.STORAGE_TYPE_PLOCAL)
    #     print("TinkerHome Database Created.")
    # except pyorient.PYORIENT_EXCEPTION as err:
    #     print(
    #         "Failed to create TinkerHome DB: %"
    #         % err)

    # 2.删除数据库
    # client.db_drop("tinkerhome")

    # 3.数据库是否存在
    # print(client.db_exists("test"))

    # 4.列出所有的数据库信息
    # print(client.db_list())

    # 5.打开一个数据库
    # client.db_open("test","root","123456")

    # 6.刷新数据库
    # client.db_reload()

    # 7.command可以执行命令和SQL
    # 8.query只能执行sql
    # client.db_open("test","root","123456")
    # client.command("create class students")
    # client.command("create property students.id integer")
    # client.command("create property students.name string")
    # client.command("insert into students(id,name) values (1,'xuyutian') ,(2,'pyy')")
    # res = client.command("select * from students") # res is a list,
    # for i in range(0,len(res)): # i is a OrientRecord
    #     # print(res[i].oRecordData.get("name"))  oRecordData is a dictionary
    # print(keys)

    # list classes
    # res = client.command("SELECT name FROM (SELECT expand(classes) FROM metadata:schema)")

    # #将获取结果转换为 DataFrame
    # list1 = []
    # for i in range(0, len(res)):
    #     o = list((res[i].oRecordData.values()))
    #     list1.append(o)
    # columns = list(res[0].oRecordData.keys())
    # df = pd.DataFrame(list1,columns=columns)
    # print(df)

    # list1=[1,2]
    # list2=["xyt","pyy"]
    # df = pd.DataFrame([list1,list2],columns=['id','name'],)
    # print(df)

    # #事务的处理
    # tx = client.tx_commit()
    # #事务的开始
    # tx.begin()
    # #将所有改变提交
    # tx.commit()
    # #事务的回滚
    # tx.rollback()

    client.close()
