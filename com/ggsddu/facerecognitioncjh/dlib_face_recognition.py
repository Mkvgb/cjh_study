# -*- encoding: utf-8 -*-
"""
@File       :   dlib_face_recognition.py    
@Contact    :   suntang.com
@Modify Time:   2021/1/4 15:41
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
from PIL import Image, ImageFile, ImageDraw
import numpy as np
import dlib
import face_recognition_models
import torchvision.transforms as transforms
# pip install torch-1.1.0-cp37-cp37m-win_amd64.whl
# pip install torchvision-0.3.0-cp37-cp37m-win_amd64.whl
from pkg_resources import resource_filename

ImageFile.LOAD_TRUNCATED_IMAGES = True
face_detector = dlib.get_frontal_face_detector()

predictor_68_point_model = resource_filename(__name__, "src/shape_predictor_68_face_landmarks_asian.dat")
pose_predictor_68_point = dlib.shape_predictor(predictor_68_point_model)

predictor_5_point_model = face_recognition_models.pose_predictor_five_point_model_location()
pose_predictor_5_point = dlib.shape_predictor(predictor_5_point_model)

cnn_face_detection_model = face_recognition_models.cnn_face_detector_model_location()
cnn_face_detector = dlib.cnn_face_detection_model_v1(cnn_face_detection_model)

face_recognition_model = resource_filename(__name__, "src/dlib_face_recognition_resnet_model_v1_asian.dat")
face_encoder = dlib.face_recognition_model_v1(face_recognition_model)


def load_image_file(file, mode='RGB'):
    """
    Loads an image file (.jpg, .png, etc) into a numpy array

    :param file: image file name or file object to load
    :param mode: format to convert the image to. Only 'RGB' (8-bit RGB, 3 channels) and 'L' (black and white) are supported.
    :return: image contents as numpy array
    """
    im = Image.open(file)
    if mode:
        im = im.convert(mode)
    return np.array(im)


def _face_encodings(face_image, known_face_locations=None, num_jitters=1, model="small"):
    """
    Given an image, return the 128-dimension face encoding for each face in the image.

    :param face_image: The image that contains one or more faces
    :param known_face_locations: Optional - the bounding boxes of each face if you already know them.
    :param num_jitters: How many times to re-sample the face when calculating encoding. Higher is more accurate, but slower (i.e. 100 is 100x slower)
    :param model: Optional - which model to use. "large" (default) or "small" which only returns 5 points but is faster.
    :return: A list of 128-dimensional face encodings (one for each face in the image)
    """
    raw_landmarks = _raw_face_landmarks(face_image, known_face_locations, model)
    return [np.array(face_encoder.compute_face_descriptor(face_image, raw_landmark_set, num_jitters)) for raw_landmark_set in raw_landmarks]


def _raw_face_landmarks(face_image, face_locations=None, model="large"):
    if face_locations is None:
        face_locations = _raw_face_locations(face_image)
    else:
        face_locations = [_css_to_rect(face_location) for face_location in face_locations]

    pose_predictor = pose_predictor_68_point

    if model == "small":
        pose_predictor = pose_predictor_5_point

    return [pose_predictor(face_image, face_location) for face_location in face_locations]


def _raw_face_locations(img, number_of_times_to_upsample=1, model="hog"):
    """
    Returns an array of bounding boxes of human faces in a image

    :param img: An image (as a numpy array)
    :param number_of_times_to_upsample: How many times to upsample the image looking for faces. Higher numbers find smaller faces.
    :param model: Which face detection model to use. "hog" is less accurate but faster on CPUs. "cnn" is a more accurate
                  deep-learning model which is GPU/CUDA accelerated (if available). The default is "hog".
    :return: A list of dlib 'rect' objects of found face locations
    """
    if model == "cnn":
        return cnn_face_detector(img, number_of_times_to_upsample)
    else:
        return face_detector(img, number_of_times_to_upsample)


def _css_to_rect(css):
    """
    Convert a tuple in (top, right, bottom, left) order to a dlib `rect` object

    :param css:  plain tuple representation of the rect in (top, right, bottom, left) order
    :return: a dlib `rect` object
    """
    return dlib.rectangle(css[3], css[0], css[1], css[2])


def weight_mean_grey_good(path):
    # 加权平均法 + PIL 进行灰度化
    im01 = Image.open(path)
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


def _trim_css_to_bounds(css, image_shape):
    """
    Make sure a tuple in (top, right, bottom, left) order is within the bounds of the image.

    :param css:  plain tuple representation of the rect in (top, right, bottom, left) order
    :param image_shape: numpy shape of the image array
    :return: a trimmed plain tuple representation of the rect in (top, right, bottom, left) order
    """
    return max(css[0], 0), min(css[1], image_shape[1]), min(css[2], image_shape[0]), max(css[3], 0)


def _rect_to_css(rect):
    """
    Convert a dlib 'rect' object to a plain tuple in (top, right, bottom, left) order

    :param rect: a dlib 'rect' object
    :return: a plain tuple representation of the rect in (top, right, bottom, left) order
    """
    return rect.top(), rect.right(), rect.bottom(), rect.left()


def _face_locations(img, number_of_times_to_upsample=1, model="hog"):
    """
    Returns an array of bounding boxes of human faces in a image

    :param img: An image (as a numpy array)
    :param number_of_times_to_upsample: How many times to upsample the image looking for faces. Higher numbers find smaller faces.
    :param model: Which face detection model to use. "hog" is less accurate but faster on CPUs. "cnn" is a more accurate
                  deep-learning model which is GPU/CUDA accelerated (if available). The default is "hog".
    :return: A list of tuples of found face locations in css (top, right, bottom, left) order
    """
    if model == "cnn":
        return [_trim_css_to_bounds(_rect_to_css(face.rect), img.shape) for face in _raw_face_locations(img, number_of_times_to_upsample, "cnn")]
    else:
        return [_trim_css_to_bounds(_rect_to_css(face), img.shape) for face in _raw_face_locations(img, number_of_times_to_upsample, model)]


def face_encodings(picture_path1):
    image = Image.open(picture_path1)
    file_load = np.array(image)
    face_locations = _face_locations(file_load, 1)
    image_transforms = transforms.Compose([transforms.Grayscale(1)])
    image = image_transforms(image).convert('RGB')
    file_load = np.array(image)
    return _face_encodings(file_load, face_locations)


picture_path1 = 'picture/xr1.jpg'
picture_path2 = 'picture/xr2.jpg'
print(np.linalg.norm(face_encodings(picture_path1)[0] - face_encodings(picture_path2), axis=1))
