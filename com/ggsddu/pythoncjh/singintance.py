# -*- encoding: utf-8 -*-
"""
@File       :   singintance.py    
@Contact    :   ggsddu.com
@Modify Time:   2020/12/29 9:28
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
# 单例模式，且初始化__init__只会执行一次，且gc机制不会回收内存
class SmsInit(object):
    __species = None
    __first_init = True

    def __init__(self):
        print('init run')
        if self.__first_init:
            self.sms_serial = 1
            self.__class__.__first_init = False

    def __new__(cls, *args, **kwargs):
        if cls.__species is None:
            cls.__species = object.__new__(cls)
        return cls.__species

# 单例只能有一个
import threading
class Singleton(object):
    _instance_lock = threading.Lock()

    def __init__(self):
        self.ser = 1


    def __new__(cls, *args, **kwargs):
        if not hasattr(Singleton, "_instance"):
            with Singleton._instance_lock:
                if not hasattr(Singleton, "_instance"):
                    Singleton._instance = object.__new__(cls)
        return Singleton._instance

obj1 = Singleton()
obj1.ser = 4
print(obj1.ser)
obj2 = Singleton()
print(obj2.ser)
print(obj1,obj2)

# def task(arg):
#     obj = Singleton()
#     print(obj)
#
# for i in range(10):
#     t = threading.Thread(target=task,args=[i,])
#     t.start()