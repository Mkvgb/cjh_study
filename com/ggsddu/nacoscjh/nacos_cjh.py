# -*- encoding: utf-8 -*-
"""
@File       :   nacoscjh.py
@Contact    :   ggsddu.com
@Modify Time:   2020/12/16 16:57
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
import nacos
import yaml
SERVER_ADDRESSES = "192.168.1.99:8848"
NAMESPACE = 'dev'
import os


def nacos_conf_quote(conf, key_list):
    for i in range(len(key_list)):
        if conf.get(key_list[i]):
            conf = conf.get(key_list[i])
    return conf


def nacos_func(client):
    # https://github.com/nacos-group/nacos-sdk-python
    # 增加观察者
    client.add_config_watcher(data_id, group)
    # 删除观察者
    client.remove_config_watcher()
    # 发布配置
    client.publish_config(data_id, group)
    # 删除配置
    client.remove_config(data_id, group)
    # 注册实例
    client.add_naming_instance()
    # 注销实例
    client.remove_naming_instance()
    # 修改实例
    client.modify_naming_instance()
    # 查询实例
    client.get_naming_instance()
    # 发送实例节拍
    client.send_heartbeat()
    # 订阅服务实例
    client.subscribe()
    # 退订服务实例
    client.unsubscribe()
    # 停止所有服务订阅
    client.stop_subscribe()
    # 调试模式
    client.set_debugging()

# nacos自定义连接方法
def nacos_join(loader, node):
    seq = loader.construct_sequence(node)
    return ''.join([str(i) for i in seq])
yaml.add_constructor('!join', nacos_join)

client = nacos.NacosClient(SERVER_ADDRESSES, namespace=NAMESPACE, username="nacos", password="ggsddu@123")
data_id = "application-dev.yml"
group = "dev"
conf_nacos = client.get_config(data_id, group)
file = '/com/test/conf_20201212.yml'
yml_conf = open(file, mode='r', encoding='utf-8').read()
conf = yaml.load(yml_conf)
print(conf['hdfs']['hosts'])
# print(conf['definitions']['test']['ref'])
# print(conf['logging'])      # 打印logging层的配置成json
# print(conf.get('logging'))  # 打印logging层的配置成json
# conf.keys()                 # 第一层的key打印出来
