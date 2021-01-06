# -*- encoding: utf-8 -*-
"""
@File       :   grey_PIL.py    
@Contact    :   suntang.com
@Modify Time:   2021/1/4 17:37
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
from PIL import Image, ImageDraw


def max_grey_bad():
    # 最大值法 + PIL 进行灰度化
    im01 = Image.open("picture/tp2.jpg")
    im02 = Image.new("L", im01.size)
    pixel = im01.load()  # cv2的图像读取后可以直接进行操作，而Image打开的图片需要加载
    w, h = im01.size
    after_table = [[0 for x in range(h)] for x in range(w)]
    draw = ImageDraw.Draw(im02)
    for i in range(im01.size[0]):
        for j in range(im01.size[1]):
            after_table[i][j] = max(pixel[i, j][0], pixel[i, j][1], pixel[i, j][2])
    try:
        for i in range(im01.size[0]):
            for j in range(im01.size[1]):
                draw.point((i, j), after_table[i][j])  # 通过表来描点画图
    except Exception as e:
        print(e)
    del draw
    im02.show()


def mean_grey_normal():
    # 平均值法 + PIL 进行灰度化
    im01 = Image.open("picture/tp2.jpg")
    im02 = Image.new("L", im01.size)
    pixel = im01.load()  # cv2的图像读取后可以直接进行操作，而Image打开的图片需要加载
    w, h = im01.size
    after_table = [[0 for x in range(h)] for x in range(w)]
    draw = ImageDraw.Draw(im02)
    for i in range(im01.size[0]):
        for j in range(im01.size[1]):
            after_table[i][j] = int((pixel[i, j][0] + pixel[i, j][1] + pixel[i, j][2]) / 3)
    try:
        for i in range(im01.size[0]):
            for j in range(im01.size[1]):
                draw.point((i, j), after_table[i][j])  # 通过表来描点画图
    except Exception as e:
        print(e)
    del draw
    im02.show()


def weight_mean_grey_good():
    # 加权平均法 + PIL 进行灰度化
    im01 = Image.open("picture/tp2.jpg")
    im02 = Image.new("L", im01.size)
    pixel = im01.load()  # cv2的图像读取后可以直接进行操作，而Image打开的图片需要加载
    w, h = im01.size
    after_table = [[0 for x in range(h)] for x in range(w)]
    draw = ImageDraw.Draw(im02)
    for i in range(im01.size[0]):
        for j in range(im01.size[1]):
            # Y = 0．3R + 0．59G + 0．11B
            # 通过Image格式打开的图片，像素格式为 RGB
            after_table[i][j] = int(0.3 * pixel[i, j][0] + 0.11 * pixel[i, j][2] + 0.59 * pixel[i, j][1])
    try:
        for i in range(im01.size[0]):
            for j in range(im01.size[1]):
                draw.point((i, j), after_table[i][j])  # 通过表来描点画图
    except Exception as e:
        print(e)
    del draw
    return im02

max_grey_bad()
mean_grey_normal()
weight_mean_grey_good()