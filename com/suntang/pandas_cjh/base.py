# -*- encoding: utf-8 -*-
"""
@File       :   base.py    
@Contact    :   suntang.com
@Modify Time:   2020/9/3 10:04
@Author     :   cjh
@Version    :   1.0
@Desciption :   None
"""
import pandas as pd
import os


def un_zip_Tree(path):                              # 解压文件夹中的zip文件
    """解压某路径下所有zip文件"""
    if not os.path.exists(path):                    # 如果本地文件夹不存在，则创建它
        os.makedirs(path)
    for file in os.listdir(path):                   #listdir()返回当前目录下清单列表
        Local = os.path.join(path, file)            #os.path.join()用于拼接文件路径
        if os.path.isdir(Local):                    # 判断是否是文件
            if not os.path.exists(Local):           #对于文件夹：如果本地不存在，就创建该文件夹
                os.makedirs(Local)
            un_zip_Tree(Local)
        else:  # 是文件
            if os.path.splitext(Local)[1] == '.log':#os.path.splitext(Remote)[1]获取文件扩展名，判断是否为.zip文件
                df = pd.read_json(path + '/' + file)
                print(path + '/' + file)
                df["file_path"] = path
                print(df)
                df.to_csv('/home/test/cjh/csv/' + '.csv', mode='a')


df = un_zip_Tree('/var/anxiang_data/thrid_20200828/renzixing/CSZL/20200721')
print(pd.read_csv('/root/test.csv'))

# file_path = '/var/anxiang_data/thrid_20200828/renzixing/CSZL/20200721/00/30/20200721003301100_145_441200_723005104_008.log'
# file_path2 = '/var/anxiang_data/thrid_20200828/renzixing/CSZL/20200721/00/30/20200721003301100_145_441200_723005104_008.log'
# df = pd.read_json(file_path)
# df2 = pd.read_json(file_path2)
# df.to_csv('/root/test.csv', mode='a', header=True)
# df2.to_csv('/root/test.csv', mode='a', header=False)
# print(pd.read_csv('/root/test.csv'))
