# -*- encoding: utf-8 -*-
"""
@File       :   face_test.py
@Contact    :   ggsddu.com
@Modify Time:   2020/11/19 11:26
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
import face_recognition
import numpy as np


# def sim(x, split=0.6):
#     split = 1 / (1 + split)
#     x = 1 / (1 + x)
#     if x <= split:
#         return x
#     else:
#         x = x - split
#         return np.sqrt((1 - split) * (1 - split) - (x - (1 - split)) * (x - (1 - split))) + split
#
# def sim2(x, split=0.6):
#     pass

# face_locations = face_recognition.face_locations(image)   # 会给出人脸在照片的位置，如果有多个人脸，会返回多个位置数组的数组

picture_of_me = face_recognition.load_image_file("picture/xr1.jpg")
my_face_encoding = face_recognition.face_encodings(picture_of_me, model='cnn')    # 返回脸型特征编码，如果没识别出人，则为空数组[]

unknown_picture = face_recognition.load_image_file("picture/xr2.jpg")
unknown_face_encoding = face_recognition.face_encodings(unknown_picture, model='cnn')[0]

results = face_recognition.face_distance(my_face_encoding, unknown_face_encoding)
print(results)
# [0.48938834]