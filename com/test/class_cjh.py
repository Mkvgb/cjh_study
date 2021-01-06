# -*- encoding: utf-8 -*-
"""
@File       :   class_cjh.py    
@Contact    :   ggsddu.com
@Modify Time:   2020/11/12 17:12
@Author     :   cjh
@Version    :   1.0
@Desciption :   None
"""


class demoClass:
    instances_created = 0

    def __new__(cls, *args, **kwargs):
        print("__new__():", cls, args, kwargs)
        instance = super().__new__(cls)
        instance.number = cls.instances_created
        cls.instances_created += 1
        return instance

    def __init__(self, attribute):
        print("__init__():", self, attribute)
        self.attribute = attribute


test1 = demoClass("abc")
test2 = demoClass("xyz")
print(test1.number, test1.instances_created)
print(test2.number, test2.instances_created)
