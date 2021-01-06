# -*- encoding: utf-8 -*-
"""
@File       :   9_face_lib_distance.py
@Contact    :   ggsddu.com
@Modify Time:   2020/11/22 15:58
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
import face_recognition
"""将未知的人物照与已知的人脸库进行比较，并打印与库里各图片的相似程度"""
# Often instead of just checking if two faces match or not (True or False), it's helpful to see how similar they are.
# You can do that by using the face_distance function.

# The model was trained in a way that faces with a distance of 0.6 or less should be a match. But if you want to
# be more strict, you can look for a smaller face distance. For example, using a 0.55 cutoff would reduce false
# positive matches at the risk of more false negatives.

# Note: This isn't exactly the same as a "percent match". The scale isn't linear. But you can assume that images with a
# smaller distance are more similar to each other than ones with a larger distance.

# Load some images to compare against
known_obama_image = face_recognition.load_image_file("picture/me.jpg")
known_biden_image = face_recognition.load_image_file("picture/liangzai.jpg")

# Get the face encodings for the known images
obama_face_encoding = face_recognition.face_encodings(known_obama_image)[0]
biden_face_encoding = face_recognition.face_encodings(known_biden_image)[0]

known_encodings = [
    obama_face_encoding,
    biden_face_encoding
]

# Load a test image and get encondings for it
image_to_test = face_recognition.load_image_file("picture/metoo.jpg")
image_to_test_encoding = face_recognition.face_encodings(image_to_test)[0]

# See how far apart the test image is from the known faces
face_distances = face_recognition.face_distance(known_encodings, image_to_test_encoding)

for i, face_distance in enumerate(face_distances):
    print("The test image has a distance of {:.2} from known image #{}".format(face_distance, i))
    print("- With a normal cutoff of 0.6, would the test image match the known image? {}".format(face_distance < 0.6))
    print("- With a very strict cutoff of 0.5, would the test image match the known image? {}".format(face_distance < 0.5))

def unknown_face_compare(input_source_images):
    # 建立暫存檔案資料夾
    path_temp_images = input_source_images + "temp_images\\"
    if os.path.exists(path_temp_images):
        shutil.rmtree(path_temp_images)
    os.mkdir(path_temp_images)
    # 進行未知影像之人臉比對
    source_images64   = []  # base64格式的未知影像(輸出HTML用)
    source_attributes = []  # 儲存未知影像之相關辨識結果([檔案名稱, 辨識人臉數, 人臉特徵值數量, 特徵值比對結果])
    for image_name in os.listdir(path=input_source_images):
        # 排除資料夾，僅處影像檔案
        if "." in image_name:
            # 將原始影像之寬度(width)縮減
            scale_image(input_source_images+image_name, path_temp_images+image_name, width=1000)
            # 將影像檔案轉為二維向量矩陣(numpy array)
            src_image      = face_recognition.load_image_file(input_source_images+image_name)
            # 辨識原始影像中的人臉區域
            face_locations = face_recognition.face_locations(src_image, number_of_times_to_upsample=1, model="cnn")
            # 產生原始影像中已識別人臉區域的特徵值
            face_encodings = face_recognition.face_encodings(src_image, known_face_locations=face_locations, num_jitters=10)
            # 針對產生出 N 組特徵值逐一比對的結果(取得最接近的辨識結果)
            match_results = []
            for i, face_encoding in enumerate(face_encodings, 0):
                # 逐一計算已知人臉與未知人臉之間的歐式距離(越小越相近)
                match_values  = list(face_recognition.face_distance(labeled_encodings, face_encoding))
                # 取得最小的歐式距離值，並對應找出姓名
                matched_index = match_values.index(min(match_values))
                matched_name  = labeled_names[matched_index]
                # 用1減去歐式距離值乘上100，用以代表相似度(越大越相似)
                match_results.append("{}({}%)".format(matched_name, round((1-match_values[matched_index])*100, 2)))
            # 組合未知影像之相關辨識結果(字串=[檔案名稱, 辨識人臉數, 特徵值比對結果])
            source_attributes.append([image_name, len(face_locations), "；".join(match_results)])
            # 在原始影像中繪製辨識出的人臉區域
            pil_image = Image.fromarray(src_image)
            draw      = ImageDraw.Draw(pil_image)
            for face_location in face_locations:
                top, right, bottom, left = face_location
                pos = (left, top, right, bottom)
                # 繪製人臉識別區域(寬度=3，顏色=RED)
                line_border = 3
                for i in range(line_border):
                    draw.rectangle(pos, outline="red")
                    pos = (pos[0]+1,pos[1]+1, pos[2]+1,pos[3]+1)
            pil_image.save((path_temp_images+image_name), "JPEG")
            # 產生輸出base64格式的影像字串(用於HTML檔案)
            b_str = scale_image(path_temp_images + image_name, width=500, toString=True)
            source_images64.append(b_str.decode('ASCII'))
    # 產生HTML程式碼內容(未知影像之辨識結果)
    output_html = ["<table border=2>"]
    for k, each_img in enumerate(source_attributes, 0):
        output_html.append("<tr><td>")
        output_html.append("<div>檔案：{}，辨識人臉數：{}，比對結果：{}。</div>".format(each_img[0], each_img[1], each_img[2]))
        output_html.append("<div><img src='data:image/jpeg;base64,{}'></div>".format(source_images64[k]))
        output_html.append("</td></tr>")
    output_html.append("</table>")
    # 刪除暫存檔案資料夾(以及所有已縮小之暫存圖片)
    if os.path.exists(path_temp_images):
        shutil.rmtree(path_temp_images)
    # 回傳處理結果：Tuple(處理影像數量, HTML輸出內容)
    return (len(source_attributes), output_html)