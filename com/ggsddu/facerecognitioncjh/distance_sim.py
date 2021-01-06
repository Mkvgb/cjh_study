# -*- encoding: utf-8 -*-
"""
@File       :   distance_sim.py    
@Contact    :   suntang.com
@Modify Time:   2020/12/30 16:21
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
import numpy as np


def sim1(distance, split=0.6):
    """距离转相似度百分比， 具体方法为在0.625后加个弓形"""
    split = 1 / (1 + split)
    distance = 1 / (1 + distance)
    print(f'tmp : {str(distance)}')
    if distance <= split:
        return distance
    else:
        distance = distance - split
        print(f'tmp x :{distance}')
        return np.sqrt((1 - split) * (1 - split) - (distance - (1 - split)) * (distance - (1 - split))) + split


def sim2(sim, split=0.6):
    """相似度百分比转距离, 未完成"""
    split = 1 / (1 + split)
    if sim <= split:
        return 1 / sim - 1
    else:
        sim = (sim - split) * (sim - split)
        return np.sqrt((1 - split) * (1 - split) - sim) + 1 - split
        # return np.sqrt(1 - 2 * split - x * x + 2 * split * x) + 1 - split
        # x = x + split
        # return 1 / x - 1

def sim5(distance):
    """距离转相似度百分比"""
    return 1 - 0.8 * distance

def sim6(sim):
    """相似度百分比转距离"""
    return (1 - sim) * 1.25