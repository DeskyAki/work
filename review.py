import json
import time
import socket
import threading
import traceback
import http.client
import urllib.request
from optparse import OptionParser
from multiprocessing import Process

saigono_result = {}
# {‘searchV7.searcher.coupon_activity_all.avg,$,cluster=ht,env=a3,platform=production’：2}  存放的是有问题的
daijyoubu = set()
# 存的是没问题的，直接写死为0
total_results = {}
# 统计一个partition中主机个数
ip_partition = {}
# 主机抓取异常，主机ip和所在partition的映射表  ！抓取不到主机【不】算作延时


def get_ip_from_opc(url, timout=2, retry=3):
    resp = None
    resp_data = []
    while retry > 0:
        try:
            resp = urllib.request.urlopen(url, timeout=timout)
            if resp.code // 100 == 2:
                resp_data = json.loads(resp.read())
                break
            else:
                retry -= 1
        except Exception:
            print('[ERROR]get_ip_from_opc:%s' % traceback.format_exc())
        if resp is None or resp.code // 100 != 2:
            print("[ERROR]get_ip_from_opc request failed!")
    return resp_data


def set_args_lst(module, platform, ModuleInfo, m_args, proxy, env):
    global ip_partition
    tmp_list = []
    # url_opc_module = f"http://opcenter.jd.com/api/v1.0/hosts/?project=searchV7&platform={platform}&module={module}&cluster={cluster}"
    # url_opc_module = f"http://opcenter.jd.com/api/v1.0/hosts/?project=searchV7&platform={platform}&module={module}"
    url_opc_module = f"http://opcenter.jd.com/api/v1.0/hosts/?project=searchV7&platform={platform}&module={module}"
    resp = get_ip_from_opc(url_opc_module)
    if resp:
        for info in resp:
            tags = f"cluster={info['cluster']},env={info['env']},platform={platform}"
            # 访问单个主机可能会出现无法访问的情况，这里建立主机ip和partition对应的哈希表
            ip_partition[info['ip']] = tags + "_" + info["partition"]
            tmp_list.extend(
                zip(
                    [ModuleInfo],
                    [info['ip']],
                    [m_args],
                    [proxy],
                    [env],
                    [tags]
                )
            )
    return tmp_list


class CommonInfo(object):

    DATA_TIME_FORMAT = "%Y%m%d%H%M%S"

    def __init__(self, ip, tags):
        self.PART_MATCH_TB = {}
        self.TOTAL_MATCH_TB = {}
        self.ip = ip
        self.current = None
        self.full = {}
        self.cmd = ""
        self.tags = tags
        self.register_handler("['timestamp']", self.handle_full)

    def register_handler(self, key, handler):
        self.PART_MATCH_TB[key] = handler

    def handle_full(self, value, partition):
        tms = time.strptime(value, self.DATA_TIME_FORMAT)
        self.full[partition] = int(time.mktime(tms))

    def parse_json(self, ip, json_info, match_info, partition, total_data):
        for key in match_info:
            try:
                value = eval("json_info%s" % key)
                if not value:
                    continue
                handler = match_info[key]
                handler(value, partition)
            except Exception:
                pass

            try:
                total_value = eval("total_data%s" % key)
                if not total_value:
                    continue
                handler = match_info[key]
                handler(total_value, partition)
            except Exception:
                continue


class SearcherInfo(CommonInfo):
    global saigono_result
    global total_results
    global data_type

    def __init__(self, ip, tags):
        super(SearcherInfo, self).__init__(ip, tags)
        for d_type, d_value in data_type.items():
            self.register_handler(d_value["path"], self.handle_data)

    def handle_data(self, d_type, value, partition):
        tms = int(time.mktime(time.strptime(value, self.DATA_TIME_FORMAT)))
        with open(json_dir, "r") as f:
            time_dic = json.load(f)
        # 从time_dic 读取最新时间戳
        current = None
        if d_type[:3] == "inc":
            current = int(time.mktime(
                time.strptime(time_dic["inc"][Module]["partition_" + str(partition)][d_type[4:]], "%Y%m%d%H%M%S")))
        elif d_type[:3] == "sen":
            current = int(time.mktime(
                time.strptime(time_dic["sensi"][Module]["partition_" + str(partition)][d_type[4:]], "%Y%m%d%H%M%S")))
        elif d_type[:3] == "cou":
            current = int(time.mktime(time.strptime(time_dic["coupon"][d_type[4:]], "%Y%m%d%H%M%S")))

        try:
            total_results[self.tags + "_" + partition] += 1
        except Exception:
            total_results[self.tags + "_" + partition] = 1

        if current - tms >= data_type[d_type]["time_rule"]:
            try:
                data_type[d_type][self.tags + "_" + partition] += 1
            except Exception:
                data_type[d_type] = {}
                data_type[d_type][self.tags + "_" + partition] = 1


class Env(object):
    def __init__(self, agent_host, agent_port, debug):
        self.agent_host = agent_host
        self.agent_port = agent_port
        self.debug = debug



class Env(object):
    def __init__(self, agent_host, agent_port, debug):
        self.agent_host = agent_host
        self.agent_port = agent_port
        self.debug = debug


def get_info(value):
    global total_results
    global data_type
    cls, ip, m_info, proxy, env, tags = value[0], value[1], value[2], value[3], value[4], value[5]

    if proxy is True:
        proxy_ip = '172.20.145.85'
        proxy_port = 80
        conn = http.client.HTTPConnection(host=proxy_ip, port=proxy_port, timeout=1)
        # conn = http.client.HTTPConnection(host=172.20.145.85, port=80, timeout=1)
        url = '{proxy}?http://{host}:{port}{info_url}'.format(proxy=m_info['proxy'], host=ip, port=m_info['port'],
                                                              info_url=m_info['url'])
        # /inner_proxy?http://11.3.192.2:8003/ServerOpsService/ops?info
    else:
        conn = http.client.HTTPConnection(host=ip, port=m_info['port'], timeout=1)
        url = m_info['url']
    # 访问
    try:
        conn.request('GET', url)
        resp = conn.getresponse()
        raw_str = resp.read()
        json_data = json.loads(raw_str)
    except Exception:
        try:
            total_results[ip_partition[ip]] += 1
        except Exception:
            total_results[ip_partition[ip]] = 1
        return -1
    info = cls(ip, tags)
    for p, data in json_data['db']['partitions'].items():
        # p = partition_28
        # p.lstrip("partition_") 28
        partition = p.lstrip("partition_")
        info.parse_json(ip, data, info.PART_MATCH_TB, partition, json_data)