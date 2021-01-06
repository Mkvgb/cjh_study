# -*- encoding: utf-8 -*-
"""
@File       :   unzip.py    
@Contact    :   ggsddu.com
@Modify Time:   2020/8/27 17:24
@Author     :   cjh
@Version    :   1.0
@Desciption :   None
"""
#!/usr/bin/env pythoncjh
#! -*- coding:utf-8 -*-


import zipfile
import os
import shutil

def un_zip(path, file_name):
    """解压单个文件,其中path是该文件的路径，file_name是该文件的名称，格式为('/home/test/', 'a.zip')"""
    if path[-1] != '/':
        path = path + '/'
    zip_file = zipfile.ZipFile(path + file_name)    #读取zip文件
    # if os.path.isdir(file_name[0:-20]):           #判断是否存在文件夹，file_name[0:20]是为了方便我去掉日期和.zip的后缀
    #     pass
    # else:
    #     os.mkdir(file_name[0:-20])                #创建文件夹

    for names in zip_file.namelist():               #解压
        zip_file.extract(names, path)
    zip_file.close()
    # Conf = os.path.join(file_name[0:-20], 'conf') # 删除配置文件，如不需要，可删除
    # shutil.rmtree(Conf)
    # if os.path.exists(file_name):                 # 如果需要删除zip文件
    #     os.remove(file_name)
    print(file_name, '解压成功')
    # os.remove(file_name)


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
            if os.path.splitext(Local)[1] == '.zip':#os.path.splitext(Remote)[1]获取文件扩展名，判断是否为.zip文件
                un_zip(path, file)                  #解压文件

def del_unzip(path, suffix):
    """由于解压后可能需要删除解压后的文件，输入的参数path为文件路径，suffix为文件后缀名"""
    if suffix[0] != '.':
        suffix = '.' + suffix
    if not os.path.exists(path):
        os.makedirs(path)
    for file in os.listdir(path):
        Local = os.path.join(path, file)
        if os.path.isdir(Local):
            if not os.path.exists(Local):
                os.makedirs(Local)
            del_unzip(Local, suffix)
        else:  # 是文件
            if os.path.splitext(Local)[1] == suffix:
                os.remove(Local)

# un_zip('/var/police_center/database/third/chengge/FJGJ/20200818/16/05/', '20200818160504897_139_441200_563985982_002.zip')
# un_zip_Tree('/var/police_center/database/third/chengge/FJGJ/20200818/16/05/')
# un_zip('/home/test/cjh/ziptest/aaa', '20200818160504897_139_441200_563985982_002.zip')
# un_zip_Tree('/home/test/cjh/ziptest/aaa')
del_unzip('/home/test/cjh/ziptest/aaa', 'log')
