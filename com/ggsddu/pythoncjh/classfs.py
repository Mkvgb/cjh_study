# -*- encoding: utf-8 -*-
"""
@File       :   classfs.py    
@Contact    :   suntang.com
@Modify Time:   2020/12/29 18:25
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
class father(object):
    def test(self, a):
        print(a)

class son(father):
    def test(self, a):
        print(a + 'a')

    def func(self):
        self.test('a')

son().func()