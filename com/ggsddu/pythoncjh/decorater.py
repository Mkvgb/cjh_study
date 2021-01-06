# -*- encoding: utf-8 -*-
"""
@File       :   decorater.py
@Contact    :   ggsddu.com
@Modify Time:   2020/12/29 16:10
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
"""@property"""
class Person(object):
    def __init__(self, name, age):
        self.name = name
        self.__age = 18

    @property
    def age(self):          # 函数名即为变量名
        return self.__age

    @age.setter
    def age(self, age):
        if age < 18:
            print('年龄必须大于18岁')
            return
        self.__age = age
        return self.__age

xm = Person('xiaoming', 20)
print(xm.age)
xm.age = 10
print(xm.age)
xm.age = 20
print(xm.age)       # 貌似唯一的用处就是判断定义的变量是否符合要求