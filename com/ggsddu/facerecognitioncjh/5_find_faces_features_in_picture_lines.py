# -*- encoding: utf-8 -*-
"""
@File       :   5_find_faces_features_in_picture_lines.py
@Contact    :   ggsddu.com
@Modify Time:   2020/11/20 15:14
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
from PIL import Image, ImageDraw
import face_recognition

# Load the jpg file into a numpy array
image = face_recognition.load_image_file("picture/tp1.jpg")

# Find all facial features in all the faces in the image
face_landmarks_list = face_recognition.face_landmarks(image)
print(face_landmarks_list)

print("I found {} face(s) in this photograph.".format(len(face_landmarks_list)))

# Create a PIL imagedraw object so we can draw on the picture
pil_image = Image.fromarray(image)
d = ImageDraw.Draw(pil_image)       # 创建一个可二次修改的图像

for face_landmarks in face_landmarks_list:
    # 将划线的所有坐标都打印出来

    # Print the location of each facial feature in this image
    for facial_feature in face_landmarks.keys():
        print("The {} in this face has the following points: {}".format(facial_feature, face_landmarks[facial_feature]))

    # Let's trace out each facial feature in the image with a line!
    for facial_feature in face_landmarks.keys():
        d.line(face_landmarks[facial_feature], width=5)

# Show the picture
pil_image.show()
