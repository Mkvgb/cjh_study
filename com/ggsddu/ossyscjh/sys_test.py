# -*- encoding: utf-8 -*-
"""
@File       :   sys_test.py    
@Contact    :   ggsddu.com
@Modify Time:   2020/12/24 9:17
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
import sys

def arg_sys():
    arg1 = sys.argv[1]
    arg2 = sys.argv[2]

def func_sys():
    print(sys.path)             # 打印系统路径, 返回模块的搜索路径，初始化时使用PYTHONPATH环境变量的值
    print(sys.version_info)     # 打印python版本
    print(sys.version)          # 打印python版本
    print(sys.platform)         # 运行的系统
    sys.stdout.write('a')       # 输出重定向，即可以多次write打印，但不会换行
    sys.exit()                  # 函数退出，但不是退出所以，主程序会截获返回

file = 'D:/code/cjh_study/com/test/log.test'
for i in range(3):
    sys.stdout.write('a')
