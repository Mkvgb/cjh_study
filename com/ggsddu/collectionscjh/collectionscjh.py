# -*- encoding: utf-8 -*-
"""
@File       :   collectionscjh.py    
@Contact    :   suntang.com
@Modify Time:   2020/12/30 17:23
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
import collections


def counter_clt():
    collections.Counter([1, 1, 2, 3]) - collections.Counter([2, 3])         # Counter({1: 2})
    list(collections.Counter([1, 1, 2, 3]) - collections.Counter([2, 3]))   # [1]


def search(file, pattern, history=5):
    previous_lines = collections.deque(maxlen=history)
    for l in file:
        if pattern in l:
            yield l, previous_lines  # 使用yield表达式的生成器函数，将搜索过程的代码和搜索结果的代码解耦
        previous_lines.append(l)


# with open(b'file.txt', mode='r', encoding='utf-8') as f:
#     for line, prevlines in search(f, 'Python', 5):
#         for pline in prevlines:
#             print(pline, end='')
#         print(line, end='')

d = collections.deque()
d.append(1)         # 添加一个元素，不用重新复制，直接操作成功
d.append("2")       # 可添加不同类型的元素
d.appendleft(3)
print(len(d))       # 可计算长度，可迭代
d.extendleft([0])   # 从左边插入队列，插入参数只能是列表，然后插入列表里各个元素
d.extend([6, 7, 8]) # 从右边插入队列，插入参数只能是列表
d2 = collections.deque('12345') # 生成队列[1, 2, 3, 4, 5],元素类型为string
d2.popleft()        # 左边第一个元素pop出
d2.pop()            # 右边元素pop出

# 在队列两端插入或删除元素时间复杂度都是 O(1) ，区别于列表，在列表的开头插入或删除元素的时间复杂度为 O(N)
d3 = collections.deque(maxlen=2)    # 设置最大长度为2
d3.append(1)
d3.append(2)
d3.append(3)                        # append不成功，长度限制为2
d3.extendleft([3])                  # 虽然最大长度为2，但如果从左边插入，会将后面的顶掉，这里变成[3, 1, 2]

d2.clear()                          # 移除列表中所有元素
d2 = collections.deque('123451')
d2.remove('1')                      # 移除第一次出现的位置
d3 = d2.copy()                      # 影分身之术
cnt = d3.count('1')                 # 计数
index = d3.index('1')               # 元素第一次出现的位置
d2.insert(0, "chl")                # 在index为0的位置之前插入某元素，不用重新赋值，直接操作成功
d2 = collections.deque('abbcd')
d2.rotate(1)                        # 向右反转，如输入负数，则向左，这里结果为[d, a, b, b, c]

d4 = collections.defaultdict(['a', 'b', 'c'])
d4['a'].append(1)
print(d4)