# -*- encoding: utf-8 -*-
"""
@File       :   anxinag_fake_perday.py
@Contact    :   ggsddu.com
@Modify Time:   2020/11/16 17:38
@Author     :   cjh
@Version    :   1.0
@Desciption :   None
"""
import os

def cmd_os_exec():
    cmd = 'ls'
    # os.system(cmd)      # 执行命令行


def listdir_os():
    file = 'D:/code/cjh_study/com/test/'
    print(os.listdir(file))


def read_os():
    file = '/com/test/conf_20201212.yml'
    fd = os.open(file, os.O_RDWR)  # 文件描述符
    print(os.read(fd, 13))
    os.write(fd, b'aaa')
    os.open("info.log", os.O_RDWR | os.O_CREATE | os.O_APPEND, 666)


def func_os():
    os.replace('1.txt', '2.txt')    # 将1.txt替换成2.txt，如果2.txt存在，则删掉原复制新
    os.rename('1.txt', '2.txt')     # 重命名
    os.remove()                     # 删除
    fd = os.open(file, os.O_RDWR)
    os.write(fd, b'aaa')            # 只能写byte字符
    print(os.name)  # posix , nt , java， 对应linux/windows/java
    fd = os.open(file, os.O_RDWR)     # 文件描述符
    os.close(fd)        # 关闭文件描述符
    os.stat(file)  # 执行系统stat调用，只有linux貌似
    os.chmod()
    os.chown()


def path_os():
    # print(os.path.join('/home', 'geoserver', 'bin'))    # 拼接路径，第一个参数得要加/，指定从哪里开始
    # print(os.path.join('/test/cjh/weihai/combine/wifi_type', '20201101'))
    # print(os.path.join('/test/cjh/weihai/combine/wifi_type/', '20201101'))
    # print(os.path.join('/test/cjh/weihai/combine/wifi_type', '/20201101'))      # no
    # print(os.path.join('/test/cjh/weihai/combine/wifi_type/', '/20201101'))     # no
    # print(os.path.join('/test/cjh/weihai/combine/wifi_type', '20201101/'))
    # print(os.path.join('/test/cjh/weihai/combine/wifi_type/', '20201101/'))
    # print(os.path.join('/test/cjh/weihai/combine/wifi_type', '/20201101/'))     # no
    # print(os.path.join('/test/cjh/weihai/combine/wifi_type/', '/20201101/'))    # no
    file = '/com/test/conf_20201212.yml'
    print(os.path.join('/2020', '*'))       # 路径拼接
    print(os.path.exists('/com/test/conf_20201212.yml'))
                                            # 是否存在文件，返回boolean
    print(type(os.path.split('/a/b/c')))    # tuple(切分成路径, 文件)
    print(os.path.abspath('conf_20201212.yml'))      # 返回绝对路径, 向上找，找到同级目录下为该名字的文件的绝对路径，录入输入conf.yml，会找到test文件
    print(os.path.basename(file))           # 返回文件名
    print(os.path.commonprefix([file, 'D:/code/cjh_study/com/test', 'D:/code/cjh_study/com']))
                                            # 返回列表中公共路径部分最长的路径
    print(os.path.dirname(file))            # 返回路径
    print(os.path.getatime(file))           # 获取最后访问时间，秒时间戳，有小数
    print(os.path.getmtime(file))           # 最后修改时间，秒时间戳，有小数
    print(os.path.getctime(file))           # 文件创建时间，秒时间戳，有小数
    print(os.path.getsize(file))            # 返回文件大小，单位字节，是大小，不是占用空间
    os.path.isabs()                         # 判断是否为绝对路径
    os.path.isfile()                        # 判断是否为文件
    os.path.isdir()                         # 判断是否为路径
    os.path.islink()                        # 判断是否为链接
    os.path.ismount()                       # 判断是否为挂载点
    print(os.path.normcase(file))           # 大小写转换，斜杠转换
    os.path.normpath()                      # 规范path字符串形式
    os.path.realpath()                      # 返回path的真实路径
    os.path.relpath([])                     # 从start开始计算相对路径
    # https://www.runoob.com/python/python-os-path.html


def read_open():
    file = '/com/test/conf_20201212.yml'
    print(open(file, mode='r', encoding='utf-8').read())


file = 'D:/code/cjh_study/com/test/'
# fd = os.open(file, os.O_RDWR)     # 文件描述符
os.chmod()
os.chown()