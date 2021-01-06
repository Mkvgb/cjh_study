# -*- encoding: utf-8 -*-
"""
@File       :   hdfsbase.py    
@Contact    :   suntang.com
@Modify Time:   2020/12/30 9:15
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
from io import BytesIO
import pandas as pd
import pyhdfs
import requests
from loguru import logger

HDFS_HOSTS = [
    '192.168.7.150:9870',
    '192.168.7.151:9870',
    '192.168.7.152:9870',
]
class HDFSManage(object):
    user = "hdfs"

    def _init_client(self):
        s = requests.session()
        s.keep_alive = False
        client = pyhdfs.HdfsClient(
            hosts=HDFS_HOSTS,
            user_name=self.user,
            requests_session=s
        )
        return client

    def rename(self, src_name, dst_name):
        client = self._init_client()
        client.rename(src_name, dst_name)

    def delete_path(self, path):
        client = self._init_client()
        try:
            client.delete(path)
        except:
            self.delete_path(path)

    def push_data(self, data, hdfs_dir):
        """推送数据"""
        is_path_exist = self.check_path_is_exist(hdfs_dir)
        if not is_path_exist:
            self.create_path(hdfs_dir)

        file_list = self.get_file_list_by_path(hdfs_dir)
        if not file_list:
            remote_file_path = f"{hdfs_dir}/0.snappy.parquet"
        else:
            remote_file_path = f"{hdfs_dir}/{len(file_list)}.snappy.parquet"
        client = self._init_client()
        client.copy_from_local(data, remote_file_path)
        return remote_file_path

    def push_csv_data(self, hdfs_dir, filename, df):
        is_path_exist = self.check_path_is_exist(hdfs_dir)
        if not is_path_exist:
            self.create_path(hdfs_dir)
        file_path = f"{hdfs_dir}/{filename}"

        is_file_exist = self.check_path_is_exist(file_path)
        client = self._init_client()
        try:
            if is_file_exist:
                csv_data = df.to_csv(index=True, header=False).encode('utf-8')
                client.append(file_path, csv_data)
            else:
                csv_data = df.to_csv(index=True, header=True).encode('utf-8')
                client.create(file_path, csv_data)
        except Exception as e:
            logger.exception(f"push_csv_data error-->{e}")
            self.push_csv_data(hdfs_dir, filename, df)

    def read_csv_to_df(self, filepath):
        client = self._init_client()
        hdfs_file = client.open(filepath)
        buf = BytesIO()
        buf.write(hdfs_file.read())
        buf.seek(0)
        df = pd.read_csv(buf)
        return df

    def read_parquet_to_df(self, filepath):
        file_entire_path = None
        file_list = self.get_file_list_by_path(filepath)
        file_name_list = [file.pathSuffix for file in file_list]
        for filename in file_name_list:
            if filename.find('parquet') != -1:
                file_entire_path = f"{filepath}/{filename}"
                break
        if file_entire_path:
            client = self._init_client()
            hdfs_file = client.open(file_entire_path)
            buf = BytesIO()
            buf.write(hdfs_file.read())
            buf.seek(0)
            df = pd.read_parquet(buf, engine='pyarrow')
            return df
        else:
            return pd.DataFrame()

    def get_file_list_by_path(self, path):
        """获取路径下的所有文件"""
        client = self._init_client()
        return client.list_status(path)

    def check_path_is_exist(self, hdfs_dir):
        """hdfs检查路径是否存在"""
        client = self._init_client()
        return client.exists(hdfs_dir)

    def create_path(self, hdfs_dir):
        """hdfs创建目录"""
        try:
            client = self._init_client()
            client.mkdirs(hdfs_dir)
        except:
            self.create_path(hdfs_dir)