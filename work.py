import json
import time
import socket
import threading
import traceback
import http.client
import urllib.request
from functools import partial
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
    global time_out

    def __init__(self, ip, tags):
        super(SearcherInfo, self).__init__(ip, tags)
        for d_type, d_value in data_type.items():
            self.register_handler(d_value["path"], partial(self.handle_data, d_type))

    def handle_data(self, d_type, value, partition):
        tms = int(time.mktime(time.strptime(value, self.DATA_TIME_FORMAT)))
        with open(json_dir, "r") as f:
            time_dic = json.load(f)
        current = None
        if d_type.split("_")[0] == "inc":
            current = int(time.mktime(
                time.strptime(time_dic["inc"][Module]["partition_" + str(partition)]["inc"], "%Y%m%d%H%M%S")))
            try:
                total_results[self.tags + "_" + partition] += 1
            except Exception:
                total_results[self.tags + "_" + partition] = 1
        elif d_type.split("_")[0] == "sensi":
            current = int(time.mktime(
                time.strptime(time_dic["sensi"][Module]["partition_" + str(partition)][d_type[6:]], "%Y%m%d%H%M%S")))
        elif d_type.split("_")[0] == "coupon":
            current = int(time.mktime(time.strptime(time_dic["coupon"][d_type[7:]], "%Y%m%d%H%M%S")))

        if current - tms >= data_type[d_type]["time_rule"]:
            try:
                time_out[d_type][self.tags + "_" + partition] += 1
            except Exception:
                time_out[d_type][self.tags + "_" + partition] = 1


class Env(object):
    def __init__(self, agent_host, agent_port, debug):
        self.agent_host = agent_host
        self.agent_port = agent_port
        self.debug = debug


def get_info(value):
    global total_results
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
    try:
        for p, data in json_data['db']['partitions'].items():
            # p = partition_28
            # p.lstrip("partition_") 28
            partition = p.lstrip("partition_")
            info.parse_json(ip, data, info.PART_MATCH_TB, partition, json_data)
    except Exception:
        # 有一部分主机没有db数据
        pass


def got_cmd(inner_module):
    cmd = ""
    global saigono_result
    global daijyoubu
    current_time = str(int(time.time()))
    for p, v in total_results.items():
        #  'cluster=ht,env=c5,platform=production_24': 3,
        # 'inc': {'cluster=zyx,env=webhospital,platform=production_0': 1,
        tmp = p.split("_")
        for d_t, d_v in time_out.items():
            try:
                value = round((d_v[p] / v), 2)
            except Exception:
                value = 0.0
            cmd += f'searchV7.{d_t}_p.avg,{int(current_time)},{value},hostname=partition_{tmp[1]},' \
                   f'{tmp[0]},module={inner_module},partition={tmp[1]}\n'
            if value >= threshold_rule:
                key = f'searchV7.{d_t}_a.avg,{current_time},$,{tmp[0]},module={inner_module}'
                try:
                    saigono_result[key] += 1
                except Exception:
                    saigono_result[key] = 1
            else:
                suki = f'searchV7.{d_t}_a.avg,{current_time},0,{tmp[0]},module={inner_module}'
                daijyoubu.add(suki)
    return cmd


def send_cmd(cmd):
    pass
    # sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # sock.settimeout(1.5)
    # sock.connect((host, port))
    # # fd = sock.makefile()
    # if debug is True:
    #     print(cmd)
    # sock.send(cmd.encode())
    # sock.close()


def main(moudle_arg):
    global Module
    Module = moudle_arg
    args_lst = []
    for platform in platform_list:
        args_lst.extend(set_args_lst(moudle_arg, platform, SearcherInfo,
                                     {"port": 8003, "url": "/ServerOpsService/ops?info", "proxy": "/inner_proxy"}, True,
                                     env))
    for i in args_lst:
        th = threading.Thread(target=get_info, args=(i, ))
        th.start()
        th.join()

    cmd = got_cmd(moudle_arg)
    return cmd


def handle_cmd(inner_cmd):
    cmd_line_all = inner_cmd.split("\n")
    n = 100
    cmd_lines = [cmd_line_all[i:i+n] for i in range(0, len(cmd_line_all), n)]
    for cmd_line in cmd_lines:
        cmd_str = ""
        for cmds in cmd_line:
           cmd_str = cmd_str + cmds + "\n"
        #print("==============================")
        #print(cmd_str)
        send_cmd(cmd_str)
        #print("==============================")


def handle_saigono_result():
    for k, v in saigono_result.items():
        tmp = k.split("$")
        result = f'{tmp[0]+  str(v) + tmp[1]}'
        send_cmd(result)


def handle_daijyoubu():
    for i in daijyoubu:
        send_cmd(i)


if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option('-c', '--config', action='store', dest='config',
                      help='config_file', metavar='CONF_FILE', default='./v7config.json',)
    (options, args) = parser.parse_args()
    if not options.config:
        parser.error("缺少配置文件，如 -c xx.config")
    config = json.load(open(options.config))
    host = config['agent_host']
    port = config['agent_port']
    platform_list = config['platform']
    module_lst = config['module']
    debug = config['debug']
    json_dir = config['json_dir']
    threshold_rule = config['threshold_rule']
    # 下面是新增的
    data_type = config['data_type']

    # 存取的是数据延时的数量新增
    time_out = dict.fromkeys(data_type, dict())

    env = Env(host, port, True)

    for module in module_lst:
        result_cmd = main(module)
        p1 = Process(target=handle_cmd, args=(result_cmd,))
        p1.start()
        p2 = Process(target=handle_daijyoubu, args=())
        p2.start()
        p3 = Process(target=handle_saigono_result, args=())
        p3.start()
        p1.join()
        p2.join()
        p3.join()
        total_results = {}
        saigono_result = {}
        daijyoubu = {}
        ip_partition = {}
        print(time_out)
        time_out = dict.fromkeys(time_out, dict())
        print(time_out)


