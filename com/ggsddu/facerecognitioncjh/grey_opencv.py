# -*- encoding: utf-8 -*-
"""
@File       :   grey_opencv.py
@Contact    :   suntang.com
@Modify Time:   2021/1/4 17:32
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
from PIL import Image, ImageDraw
import numpy as np
import cv2

def max_grep_bad():
    # 最大值法 + PIL 进行灰度化
    im01 = Image.open('picture/tp2.jpg')
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
    im01.show()
    im02.show()


def mean_grep_normal():
    # 平均值法 + cv2 进行灰度化
    image = cv2.imread('picture/tp2.jpg')
    h, w = image.shape[:2]
    gray_img = np.zeros((h, w), dtype=np.uint8)
    for i in range(h):
        for j in range(w):
            gray_img[i, j] = (int(image[i, j][0]) + int(image[i, j][1]) + int(image[i, j][2])) / 3
    cv2.imshow("gray_image", gray_img)
    cv2.waitKey(0)


def weight_mean_grep_good():
    # 加权平均法 + cv2 进行灰度化
    image = cv2.imread('picture/tp2.jpg')
    h, w = image.shape[:2]
    gray_img = np.zeros((h, w), dtype=np.uint8)
    for i in range(h):
        for j in range(w):
            # Y = 0．3R + 0．59G + 0．11B
            # 通过cv格式打开的图片，像素格式为 BGR
            gray_img[i, j] = 0.3 * image[i, j][2] + 0.11 * image[i, j][0] + 0.59 * image[i, j][1]
    cv2.imshow("gray_image", gray_img)
    cv2.waitKey(0)
