import keyword
# 此乃注释
'''此乃注释'''
"""此乃注释"""
print("abc")
print(keyword.kwlist)
print("ab\
c")
print ("我叫 %s 今年 %d 岁!" % ('小明', 10))

# 条件循环语句
while():
    continue
    break
else:
    pass

for letter in 'Python':  # 第一个实例
    print('当前字母 :', letter)
else:
    pass

fruits = ['banana', 'apple', 'mango']
for fruit in fruits:  # 第二个实例
    print('当前水果 :', fruit)
for index in range(len(fruits)):
  print('当前水果 :', fruits[index])

# 字符串
word = '字符串'
sentence = "句子"
paragraph = """段落，
可多行"""
# https://www.runoob.com/python3/python3-string.html    字符串函数

# str='Runoob'
# print(str)                 # 输出字符串
# print(str[0:-1])           # 输出第一个到倒数第二个的所有字符
# print(str[0])              # 输出字符串第一个字符
# print(str[2:5])            # 输出从第三个开始到第五个的字符
# print(str[2:])             # 输出从第三个开始的后的所有字符
# print(str * 2)             # 输出字符串两次
# print(str + '你好')          # 连接字符串
# print('hello\nrunoob')      # 使用反斜杠(\)+n转义特殊字符
# print(r'hello\nrunoob')     # 在字符串前面添加一个 r，表示原始字符串，不会发生转义
# print(str[-1], str[-6])     # 输出倒一位和倒六位，中间空格隔开

# 变量的定义与赋值
# 不可变数据（四个）：Number（数字）、String（字符串）、Tuple（元组）、Sets（集合）；
# 可变数据（两个）：List（列表）、Dictionary（字典）。
counter = 100          # 整型变量
miles = 1000.0       # 浮点型变量
name = "runoob"     # 字符串

# 多个变量赋值：
a1 = b1 = c1 = 1 ; a2, b2, c2 = 1, 2, "runoob"

# 判断变量类型
a = 1
# print(type(a))              # <class 'int'>
# print(isinstance(a, int))   # True

# class A:
#     pass
# class B(A):
#     pass
# isinstance(B(), A)    # returns True
# type(B()) == A        # returns False
# type()不会认为子类是一种父类类型。
# isinstance()会认为子类是一种父类类型。

# 数值运算
a3 = 5 + 4 ; a4 = 4.3 - 2 ; a5 = 3 * 7 ; a6 = 2 / 4 ; a7 = 17 % 3 ; a8 = 2 ** 5

# List（列表）
# 列表元素可改变
list = ['abc', 1, 1.2, "abc"]
tinylist = [123, 'runoob']
# print(list)            # 输出完整列表
# print(list[0])         # 输出列表第一个元素
# print(list[1:3])       # 从第二个开始输出到第三个元素
# print(list[2:])        # 输出从第三个元素开始的所有元素
# print(tinylist * 2)    # 输出两次列表
# print(list + tinylist) # 连接列表

# Tuple（元组）
# 列表元素不可改变
tuple = ('abcd', 786 , 2.23, 'runoob', 70.2)
tinytuple = (123, 'runoob')
tup1 = ()    # 空元组
tup2 = (20,) # 一个元素，需要在元素后添加逗号
# print (tuple)             # 输出完整元组
# print (tuple[0])          # 输出元组的第一个元素
# print (tuple[1:3])        # 输出从第二个元素开始到第三个元素
# print (tuple[2:])         # 输出从第三个元素开始的所有元素
# print (tinytuple * 2)     # 输出两次元组
# print (tuple + tinytuple) # 连接元组

# Set（集合）
# 去重
# 可以使用大括号 { } 或者 set() 函数创建集合，创建一个空集合必须用 set() 而不是 { }，因为 { } 是用来创建一个空字典。
student = {'Tom', 'Jim', 'Mary', 'Tom', 'Jack', 'Rose'}
# print(student)   # 输出集合，重复的元素被自动去掉
# 成员测试
if('Rose' in student) :
    print('Rose 在集合中')
# set可以进行集合运算
# a = set('abracadabra')
# b = set('alacazam')
# print(a)
# print(a - b)     # a和b的差集
# print(a | b)     # a和b的并集
# print(a & b)     # a和b的交集
# print(a ^ b)     # a和b中不同时存在的元素

# Dictionary(字典)
# kv
dict = {}
dict['one'] = "1 - 菜鸟教程"
dict[2]     = "2 - 菜鸟工具"
tinydict = {'name': 'runoob','code':1, 'site': 'www.runoob.com'}
# print (dict['one'])       # 输出键为 'one' 的值
# print (dict[2])           # 输出键为 2 的值
# print (tinydict)          # 输出完整的字典
# print (tinydict.keys())   # 输出所有键
# print (tinydict.values()) # 输出所有值

# 列表操作
# http://www.runoob.com/python3/python3-list.html 函数
# Python 表达式	                             结果	            描述
# len([1, 2, 3])	                          3	                长度
# [1, 2, 3] + [4, 5, 6]	         [1, 2, 3, 4, 5, 6]	            组合
# ['Hi!'] * 4	            ['Hi!', 'Hi!', 'Hi!', 'Hi!']	    重复
# 3 in [1, 2, 3]	                         True	      元素是否存在于列表中
# for x in [1, 2, 3]: print(x, end=" ")	    1 2 3	            迭代

# 元组操作
#
# 元组中的元素值是不允许删除的，但我们可以使用del语句来删除整个元组：del tup;
# Python 表达式	                                结果	                描述
# len((1, 2, 3))	                                 3	            计算元素个数
# (1, 2, 3) + (4, 5, 6)	                (1, 2, 3, 4, 5, 6)	        连接
# ('Hi!',) * 4	                    ('Hi!', 'Hi!', 'Hi!', 'Hi!')	复制
# 3 in (1, 2, 3)	                               True	            元素是否存在
# for x in (1, 2, 3): print (x,end="")	       1 2 3	            迭代

# 字典操作
# http://www.runoob.com/python3/python3-dictionary.html 字典函数
# dict['Age'] = 8;               # 更新 Age
# 删除字典元素：
# del dict['Name'] # 删除键 'Name'
# dict.clear()     # 清空字典
# del dict         # 删除字典

# 逻辑运算符：
a = 1 ; b = 0
if ( a and b ):
    if ( a or b ):
        if ( not a ):
            a = 1

# 成员运算符：//字符串，列表或元组
if ( a in list ):
    if ( b not in list ):
        b = 1

# 类型转换
x = 1
int(x)                  # 将x转换为一个整数
long(x)                 # 将x转换为一个长整数
float(x )               # 将x转换到一个浮点数
complex(x)              # 创建一个复数
str(x )                 # 将对象 x 转换为字符串
repr(x )                # 将对象 x 转换为表达式字符串
eval(str )              # 用来计算在字符串中的有效Python表达式,并返回一个对象
tuple(s )               # 将序列 s 转换为一个元组
list(s )                # 将序列 s 转换为一个列表
chr(x )                 # 将一个整数转换为一个字符
unichr(x )              # 将一个整数转换为Unicode字符
ord(x )                 # 将一个字符转换为它的整数值
hex(x )                 # 将一个整数转换为一个十六进制字符串
oct(x )                 # 将一个整数转换为一个八进制字符串

#数学模块
import math
abs(x)	    # 返回数字的绝对值，如abs(-10) 返回 10
ceil(x)	    # 返回数字的上入整数，如math.ceil(4.1) 返回 5
cmp(x, y)	# 如果 x < y 返回 -1, 如果 x == y 返回 0, 如果 x > y 返回 1
exp(x)	    # 返回e的x次幂(ex),如math.exp(1) 返回2.718281828459045
fabs(x)	    # 返回数字的绝对值，如math.fabs(-10) 返回10.0
floor(x)	# 返回数字的下舍整数，如math.floor(4.9)返回 4
log(x)	    # 如math.log(math.e)返回1.0,math.log(100,10)返回2.0
log10(x)	# 返回以10为基数的x的对数，如math.log10(100)返回 2.0
max(x1, x2,...)	# 返回给定参数的最大值，参数可以为序列。
min(x1, x2,...)	# 返回给定参数的最小值，参数可以为序列。
modf(x)	        # 返回x的整数部分与小数部分，两部分的数值符号与x相同，整数部分以浮点型表示。
pow(x, y)	    # x**y 运算后的值。
round(x ,n)	    # 返回浮点数x的四舍五入值，如给出n值，则代表舍入到小数点后的位数。
sqrt(x)	        # 返回数字x的平方根

# 随机函数
choice(seq)	# 从序列的元素中随机挑选一个元素，比如random.choice(range(10))，从0到9中随机挑选一个整数。
randrange (start, stop ,step)	# 从指定范围内，按指定基数递增的集合中获取一个随机数，基数默认值为 1
random()	    # 随机生成下一个实数，它在[0,1)范围内。
seed([x])	    # 改变随机数生成器的种子seed。如果你不了解其原理，你不必特别去设定seed，Python会帮你选择seed。
shuffle(lst)	# 将序列的所有元素随机排序
uniform(x, y)	# 随机生成下一个实数，它在[x,y]范围内。

# range用法
# 如果你需要遍历数字序列，可以使用内置range()函数。它会生成数列，例如:
# for i in range(5):print(i)//输出01234
# range(5,9)//输出5678
# range(0, 10, 3)//输出0369
# a = ['Google', 'Baidu', 'Runoob', 'Taobao', 'QQ']
# range(len(a))//输出Google,Baidu,Runoob,Taobao,QQ
# 使用range()函数来创建一个列表：
# list(range(5))
# [0, 1, 2, 3, 4]

# 迭代器
# 迭代器对象从集合的第一个元素开始访问，直到所有的元素被访问完结束。迭代器只能往前不会后退。
# 迭代器有两个基本的方法：iter() 和 next()。
# list=[1,2,3,4]
# it = iter(list)    # 创建迭代器对象
# for x in it:
#     print (x, end=" ")
# -----------------
# import sys         # 引入 sys 模块
# list=[1,2,3,4]
# it = iter(list)    # 创建迭代器对象
# while True:
#     try:
#         print (next(it))
#     except StopIteration:
#         sys.exit()

# Python3 函数:
# def 函数名（参数列表）:
#     函数体//return
#
# pythoncjh 传不可变对象实例：
# def ChangeInt( a ):
#     a = 10
# b = 2
# ChangeInt(b)
# print( b ) # 结果是 2
# 实例中有 int 对象 2，指向它的变量是 b，在传递给 ChangeInt 函数时，按传值的方式复制了变量 b，
# a 和 b 都指向了同一个 Int 对象，在 a=10 时，则新生成一个 int 值对象 10，并让 a 指向它。
#
# 函数参数的使用不需要使用指定顺序：
# def printinfo( name, age ):
# printinfo( age=50, name="runoob" )
#
# 声明函数时，参数中星号(*)可以单独出现，例如:
# def f(a,b,*,c):
# 用法：星号（*）后的参数必须用关键字传入。
# f(1,2,c=3) # 正常
#
# 不定长参数：
# 加了星号（*）的变量名会存放所有未命名的变量参数。如果在函数调用时没有指定参数，它就是一个空元组。
# 我们也可以不向函数传递未命名的变量。如下实例：
# 代码：
# def printinfo( arg1, *vartuple ):
#    print (arg1)
#    for var in vartuple:
#       print (var)
#    return
# printinfo( 70, 60, 50 )
# 输出：
# 70
# 60
# 50
#
# global 和 nonlocal关键字
# 当内部作用域想修改外部作用域的变量时，就要用到global和nonlocal关键字了。
# num = 1
# def fun1():
#     global num  # 需要使用 global 关键字声明
# -------------
# def outer():
#     num = 10
#     def inner():
#         nonlocal num   # nonlocal关键字声明

# Python3 数据结构：
# 将列表当做堆栈使用，将列表当作队列使用，遍历技巧
# http://www.runoob.com/python3/python3-data-structure.html
