# -*- encoding: utf-8 -*-
"""
@File       :   1_find_faces_in_pc.py
@Contact    :   ggsddu.com
@Modify Time:   2020/11/19 11:54
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
from PIL import Image
import face_recognition
from pathlib import Path

# # Load the jpg file into a numpy array
# path1 = Path("192.168.7.151:/home/test/cjh/tp1.jpg")
# # image = face_recognition.load_image_file("picture/yurenzhong.jpg")
# image = face_recognition.load_image_file(path1)
# face_locations = face_recognition.face_locations(image)     # 返回元组数组[(166, 540, 506, 201), (166, 540, 506, 201)]
# a = face_recognition.face_encodings(face_recognition.load_image_file("picture/yurenzhong.jpg"))
# a = [x[0] for x in a]
# print(a)
# print("I found {} face(s) in this photograph.".format(len(face_locations)))
#
# for i in range(len(face_locations)):
#
#     # Print the location of each face in this image
#     top, right, bottom, left = face_locations[i]
#     print("A face is located at pixel location Top: {}, Left: {}, Bottom: {}, Right: {}".format(top, left, bottom, right))
#
#     # You can access the actual face itself like this:
#     face_image = image[top:bottom, left:right]      # 裁剪图片
#     pil_image = Image.fromarray(face_image)         # 生成新图像，即facedraw，好生成新图像
#     # pil_image.show()

f1 = face_recognition.load_image_file("picture/tp1.jpg")
f2 = face_recognition.load_image_file('picture/tp7.jpg')
e1 = face_recognition.face_encodings(f1)
e2 = face_recognition.face_encodings(f2)
l1 = list(e1[0])
l2 = list(e2[0])
sum = 0
for i in range(128):
    sum += abs(l1[i] - l2[i])
print(sum)