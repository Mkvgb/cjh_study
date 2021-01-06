# -*- encoding: utf-8 -*-
"""
@File       :   face_recongition_utils.py
@Contact    :   ggsddu.com
@Modify Time:   2020/11/24 16:16
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
import random

import numpy
from PIL import Image
import face_recognition
from loguru import logger
from PIL import Image, ImageDraw


class StFaceRecognition(object):
    def __init__(self):
        pass

    def face_load_file(self, file_path):
        """传入图片，返回face_recongition所支持的处理格式"""
        return face_recognition.load_image_file(file_path)

    def face_encodes(self, image):
        image = self._image_or_path(image)
        return face_recognition.face_encodings(image)

    def face_encodes_compare_double_unknow_return_boolean(self, unknown1, unknown2):
        """输入两张未知人物图片的路径，如图片中有同个人返回True，无False"""
        unknown1_image = self.face_load_file(unknown1)
        face_encodings_unknown1 = face_recognition.face_encodings(unknown1_image)
        unknown2_image = self.face_load_file(unknown2)
        face_encodings_unknown2 = face_recognition.face_encodings(unknown2_image)
        if len(face_encodings_unknown1) == 0:
            logger.info("Can not find face in picture " + unknown1)
            return False
        if len(face_encodings_unknown2) == 0:
            logger.info("Can not find face in picture " + unknown2)
            return False
        for i in range(len(face_encodings_unknown1)):
            for j in range(len(face_encodings_unknown2)):
                if True in face_recognition.compare_faces([face_encodings_unknown1[i]], face_encodings_unknown2[j]):
                    return True
        return False

    def face_encodes_compare_double_unknow_return_picture(self, unknown1, unknown2):
        """输入两张未知人物图片的路径，如图片中有同个人，返回原图并标出相同的人脸，并return特征码，无则不返回"""
        unknown1_image = self.face_load_file(unknown1)
        face_locations_unknown1 = face_recognition.face_locations(unknown1_image)
        face_encodings_unknown1 = face_recognition.face_encodings(unknown1_image, face_locations_unknown1)
        pil_image1 = Image.fromarray(unknown1_image)
        draw1 = ImageDraw.Draw(pil_image1)
        unknown2_image = self.face_load_file(unknown2)
        face_locations_unknown2 = face_recognition.face_locations(unknown2_image)
        face_encodings_unknown2 = face_recognition.face_encodings(unknown2_image, face_locations_unknown2)
        pil_image2 = Image.fromarray(unknown2_image)
        draw2 = ImageDraw.Draw(pil_image2)
        if len(face_encodings_unknown1) == 0:
            logger.info("Can not find face in picture " + unknown1)
            return False
        if len(face_encodings_unknown2) == 0:
            logger.info("Can not find face in picture " + unknown2)
            return False
        compare_result = []

        for i in range(len(face_encodings_unknown1)):
            for j in range(len(face_encodings_unknown2)):
                if True in face_recognition.compare_faces([face_encodings_unknown1[i]], face_encodings_unknown2[j], tolerance=0.6):
                    compare_result.append([i, j])
        compare_cnt = len(compare_result)
        compare_span = 230 / compare_cnt
        encodings_dict = dict()

        for i in range(compare_cnt):
            color = int(10 + i * compare_span)
            top1, right1, bottom1, left1 = face_locations_unknown1[compare_result[i][0]]
            top2, right2, bottom2, left2 = face_locations_unknown2[compare_result[i][1]]
            encodings_dict[unknown1 + "_" + str(i+1)] = list(face_encodings_unknown1[compare_result[i][0]])
            encodings_dict[unknown2 + "_" + str(i+1)] = list(face_encodings_unknown2[compare_result[i][1]])
            draw1.rectangle(((left1, top1), (right1, bottom1)), outline=color, width=2)
            text_width, text_height = draw1.textsize(str(i+1))
            draw1.rectangle(((left1, bottom1 - text_height - 10), (left1 + 16, bottom1)), fill=color, outline=color)
            draw1.text((left1 + 6, bottom1 - text_height - 5), str(i+1), fill=(255, 255, 255, 255))

            draw2.rectangle(((left2, top2), (right2, bottom2)), outline=color, width=2)
            text_width, text_height = draw2.textsize(str(i+1))
            draw2.rectangle(((left2, bottom2 - text_height - 10), (left2 + 16, bottom2)), fill=color, outline=color)
            draw2.text((left2 + 6, bottom2 - text_height - 5), str(i+1), fill=(255, 255, 255, 255))
        del draw1
        del draw2
        pil_image1.show()
        pil_image2.show()
        # pil_image1.save('picture/compare_result.jpg')
        return encodings_dict

    def face_encodes_compare_known_unknown(self, known_image, unknown_image, known_is_encodes_flag=False, known_is_dict_flag=False):  # 待完善
        """known_is_encodes_flag:boolean：输入的know_image是否为脸码列表的标志，默认为False\n
        进行对比时，第一个肯定得为脸码列表，但第二个参数得为某一脸码，最后结果返回[False, True]，数组元素数与脸码列表个数一样"""
        name_list = []
        if known_is_encodes_flag and known_is_dict_flag:     # 已知人脸时特征码列表还是姓名：特征码的字典，这两个不能同时为True
            logger.info("known_is_encodes_flag and know_is_dict_flag can not be True in the same time!")
        if known_is_encodes_flag:                           # 为特征码列表
            pass
        elif known_is_dict_flag:                            # 为姓名特征码字典
            name_list = list(known_image.keys())
            known_image = list(known_image.values())
        else:                                               # 啥都不是
            known_image = self.face_encodes(known_image)
        known_faces_num = len(self.face_encodes(unknown_image))
        if known_faces_num >= 1:
            return self._face_encodes_compare_one_known(unknown_image, known_image, known_is_dict_flag, name_list)
        elif known_faces_num == 0:
            logger.info("can`t find face in known picture！")
            return 0

    def _face_encodes_compare_one_known(self, unknown_image, known_image, known_is_dict_flag, name_list):
        return_compare_result = []
        unknown_image = self.face_encodes(unknown_image)
        for j in range(len(unknown_image)):
            compare_result = face_recognition.compare_faces(known_image, unknown_image[j])     # compare     tolerance=0.4463
            if known_is_dict_flag:      # 如果为姓名特征码字典，则指返回未知图片识别的人姓名
                for i in range(len(compare_result)):
                    if compare_result[i]:
                        compare_result[i] = name_list[i]
                compare_result = list(set(compare_result))
                compare_result.remove(False)
                if len(compare_result) == 0:
                    logger.info("picture match nobody")
                    return_compare_result.append("person" + str(j+1) + " : nobody")
                    continue
                compare_result = ' or '.join(compare_result)
                return_compare_result.append("person" + str(j+1) + " : " + compare_result)
                logger.info("person" + str(j+1) + " in picture is " + compare_result)
        return return_compare_result

    def faces_location(self, image, show_flag=False, cnn_flag=False):
        """image传入图片路径或者load_file返回的数据，show_flag表示是否显示裁剪的图片"""
        image = self._image_or_path(image)
        if type(image) == str:
            image = face_recognition.load_image_file(image)
        else:
            pass
        if cnn_flag:
            face_locations = face_recognition.face_locations(image, number_of_times_to_upsample=0, model="cnn")
        else:
            face_locations = face_recognition.face_locations(image)  # 返回元组数组[(166, 540, 506, 201), (166, 540, 506, 201)]
        logger.info("Find {} face(s) in this photograph.".format(len(face_locations)))

        if show_flag == False:
            pass
        else:
            for face_location in face_locations:
                top, right, bottom, left = face_location
                face_image = image[top:bottom, left:right]  # 裁剪图片
                pil_image = Image.fromarray(face_image)  # 生成新图像，即facedraw，好生成新图像
                pil_image.show()
        return face_locations

    def faces_location_vedio(self, vedio_path):
        """每帧一个图片，待调"""
        import cv2
        video_capture = cv2.VideoCapture(vedio_path)
        frames = []
        frame_count = 0
        while video_capture.isOpened():
            # Grab a single frame of video
            ret, frame = video_capture.read()
            # Bail out when the video file ends
            if not ret:
                break
            # Convert the image from BGR color (which OpenCV uses) to RGB color (which face_recognition uses)
            frame = frame[:, :, ::-1]
            # Save each frame of the video to a list
            frame_count += 1
            frames.append(frame)
            print(frames)
            # Every 128 frames (the default batch size), batch process the list of frames to find faces
            if len(frames) == 128:
                batch_of_face_locations = face_recognition.batch_face_locations(frames, number_of_times_to_upsample=0)
                # Now let's list all the faces we found in all 128 frames
                for frame_number_in_batch, face_locations in enumerate(batch_of_face_locations):
                    number_of_faces_in_frame = len(face_locations)
                    frame_number = frame_count - 128 + frame_number_in_batch
                    logger.info("I found {} face(s) in frame #{}.".format(number_of_faces_in_frame, frame_number))
                    for face_location in face_locations:
                        # Print the location of each face in this frame
                        top, right, bottom, left = face_location
                        logger.info(" - A face is located at pixel location Top: {}, Left: {}, Bottom: {}, Right: {}".format(top, left, bottom, right))
                # Clear the frames array to start the next batch
                frames = []

    def face_landmark_feature(self, image, show_flag=False, print_landmarks=False):
        """ show_flag:boolean ：是否显示描好边的图片
            print_landmarks:boolean：是否打印landmarks"""
        from PIL import Image, ImageDraw
        # Load the jpg file into a numpy array
        image = self._image_or_path(image)
        # Find all facial features in all the faces in the image
        face_landmarks_list = face_recognition.face_landmarks(image)
        logger.info("I found {} face(s) in this photograph.".format(len(face_landmarks_list)))
        if print_landmarks:
            # [{'chin': [(215, 115), (214, 134)], 'left_eyebrow': [(239, 106)],'right_eyebrow':[],'nose_bridge':,'nose_tip':,'left_eye':,'right_eye':,'top_lip':,'bottom_lip':}]
            logger.info("landmarks : " + face_landmarks_list)
        if show_flag:
            # Create a PIL imagedraw object so we can draw on the picture
            pil_image = Image.fromarray(image)
            d = ImageDraw.Draw(pil_image)  # 创建一个可二次修改的图像
            for face_landmarks in face_landmarks_list:
                # 将划线的所有坐标都打印出来
                # Print the location of each facial feature in this image
                # for facial_feature in face_landmarks.keys():
                #     print("The {} in this face has the following points: {}".format(facial_feature, face_landmarks[facial_feature]))
                # Let's trace out each facial feature in the image with a line!
                for facial_feature in face_landmarks.keys():
                    d.line(face_landmarks[facial_feature], width=5)
            # Show the picture
            pil_image.show()

    def _image_or_path(self, arg):
        if type(arg) == str:
            return self.face_load_file(arg)
        else:
            return arg


s = StFaceRecognition()
tp1_encode = s.face_encodes('picture/tp1.jpg')[0]
tp2_encode = s.face_encodes('picture/tp2.jpg')[0]
tp3_encode = s.face_encodes('picture/tp3.jpg')[0]
tp4_encode = s.face_encodes('picture/tp4.jpg')[0]
tp5_encode = s.face_encodes('picture/tp5.jpg')[0]
tp6_encode = s.face_encodes('picture/tp6.jpg')[0]
tp7_encode = s.face_encodes('picture/tp7.jpg')[0]
tp8_encode = s.face_encodes('picture/tp8.jpg')[0]
tpbc_encode = s.face_encodes('picture/tp_bc.jpg')[0]
liang_encode = s.face_encodes('picture/liangzai.jpg')[0]
cjh_encode = s.face_encodes('picture/me.jpg')[0]
# face_dict = {"train": tp1_encode, "liang": liang_encode, "cjh": cjh_encode}
face_dict = {"trump": tp1_encode, "liang": liang_encode}
# face_list = [tp1_encode, liang_encode, cjh_encode]
face_list = [tp1_encode, tp2_encode, liang_encode]
tpz_encode = (tp1_encode + tp2_encode + tp3_encode + tp4_encode + tp5_encode + tp6_encode + tp7_encode + tp8_encode + tpbc_encode) / 9
# print(type  (tp1_encode))
# print(len(tp2_encode))
# 比较就是求欧几里得距离，用face_recognition是设的tolerance就是这个参数，比如tp1和tp2的距离是0.4464，默认tolerance为0.6，小于所以成功，如果设为0.4则失败
ec_list = [tp1_encode,
           tp2_encode,
           tp3_encode,
           tp4_encode,
           tp5_encode,
           tp6_encode,
           tp7_encode,
           tp8_encode,
           tpbc_encode]
ec_list = [list(x) for x in ec_list]


# z_list = list(tpz_encode)
# for i in range(9):
#     sum = 0
#     for j in range(128):
#         sum += abs(ec_list[i][j] - list(liang_encode)[j])
#     print(sum)
# ec_map = []

# for i in range(9):
#     for j in range(9):
#         a = numpy.linalg.norm((ec_list[i] + ec_list[j]) / 2 - tp8_encode)
#         ec_map.append(a)
# # for i in range(9):
# #     print(ec_map[i])
# print(ec_map)

# tp_encode = (tp1_encode + tp4_encode)/2
# for i in range(7):
#     dist = numpy.linalg.norm(tp4_encode - (ec_list[i] + ec_list[i + 1] + ec_list[i + 2])/3)
#     print(dist)

# dist = numpy.linalg.norm(tpz_encode-tp6_encode)
# print(dist)

# json.dumps(face_encoding.tolist())
# np.array(json.loads(face_encoding.decode("utf-8")))

# print(s.face_encodes_compare_known_unknown(face_dict, 'picture/tp2.jpg', False, True))
print(s.face_encodes_compare_double_unknow_return_picture('picture/tb1.png', 'picture/tb3.jpg'))
# clf = svm.SVC(gamma='scale')
# clf.fit(face_list, ['trump', 'trump', 'liang'])
# name = clf.predict([tp1_encode])
# print(*name)