# -*- encoding: utf-8 -*-
"""
@File       :   7_face_lib_compare.py
@Contact    :   ggsddu.com
@Modify Time:   2020/11/22 15:50
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
import face_recognition

# 将一些脸的码划入数组，并与未知脸型进行对比，以确实是否为已知脸型
# Load the jpg files into numpy arrays
biden_image = face_recognition.load_image_file("picture/xr1.jpg")
obama_image = face_recognition.load_image_file("picture/xr2.jpg")
unknown_image = face_recognition.load_image_file("picture/xr2.jpg")
# Get the face encodings for each face in each image file
# Since there could be more than one face in each image, it returns a list of encodings.
# But since I know each image only has one face, I only care about the first encoding in each image, so I grab index 0.
try:
    biden_face_encoding = face_recognition.face_encodings(biden_image)[0]
    obama_face_encoding = face_recognition.face_encodings(obama_image)[0]
    unknown_face_encoding = face_recognition.face_encodings(unknown_image)[0]
    a = face_recognition.face_encodings(face_recognition.load_image_file("picture/yurenzhong.jpg"))
except IndexError:
    print("I wasn't able to locate any faces in at least one of the images. Check the image files. Aborting...")
    quit()

known_faces = [
    biden_face_encoding,
    obama_face_encoding
]

# results is an array of True/False telling if the unknown face matched anyone in the known_faces array
# 第一个参数肯定得为人脸码数组，第二个参数肯定得为人脸码
results = face_recognition.compare_faces(known_faces, unknown_face_encoding)
print(results)

print("Is the unknown face a picture of Biden? {}".format(results[0]))
print("Is the unknown face a picture of Obama? {}".format(results[1]))
print("Is the unknown face a new person that we've never seen before? {}".format(not True in results))