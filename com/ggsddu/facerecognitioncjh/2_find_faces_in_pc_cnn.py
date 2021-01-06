# -*- encoding: utf-8 -*-
"""
@File       :   2_find_faces_in_pc_cnn.py
@Contact    :   ggsddu.com
@Modify Time:   2020/11/19 13:52
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
from PIL import Image
import face_recognition

image = face_recognition.load_image_file("picture/liangzai.jpg")

face_locations = face_recognition.face_locations(image, number_of_times_to_upsample=0, model="cnn")

print("I found {} face(s) in this photograph.".format(len(face_locations)))

print(face_locations)

for face_location in face_locations:

    # Print the location of each face in this image
    top, right, bottom, left = face_location
    print("A face is located at pixel location Top: {}, Left: {}, Bottom: {}, Right: {}".format(top, left, bottom, right))

    # You can access the actual face itself like this:
    face_image = image[top:bottom, left:right]
    pil_image = Image.fromarray(face_image)
    pil_image.show()
