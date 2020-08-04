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
inc = {}
# 统计一个partition中主机inc延时个数， 以下逻辑相同
promotion_address = {}
delivery_dict = {}
activity = {}
coupon = {}
ecard = {}
financial = {}
rebate = {}
scene = {}
# 以下为新增
promotion_icon = {}
pop_par_stock = {}
sku_penalty_v1 = {}
store_stock_v2 = {}
wjex = {}
wjtag = {}
cover_area = {}
store_time_status_v2 = {}
store_route = {}
query_promotion = {}
shop_penalty = {}
jp_warehouse_county = {}


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

    # def handle_current_time(self, value):
    #     self.current = int(value)

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
    global inc
    global promotion_address
    global delivery_dict
    global activity
    global coupon
    global ecard
    global financial
    global rebate
    global scene
    global promotion_icon
    global pop_par_stock
    global sku_penalty_v1
    global store_stock_v2
    global store_route
    global wjex
    global wjtag
    global cover_area
    global store_time_status_v2
    global store_route
    global query_promotion
    global shop_penalty
    global jp_warehouse_county

    def __init__(self, ip, tags):
        super(SearcherInfo, self).__init__(ip, tags)

        self.register_handler("['latest']['inc']['inc']", self.handle_inc)

        self.register_handler("['latest']['sensi_index']['promotion_address']", self.handle_promotion_address)
        self.register_handler("['latest']['sensi_dict']['delivery_dict']", self.handle_delivery_dict)
        # 新加
        self.register_handler("['latest']['sensi_index']['promotion_icon']", self.handle_promotion_icon)
        self.register_handler("['latest']['sensi_dict']['pop_par_stock']", self.handle_pop_par_stock)
        self.register_handler("['latest']['sensi_dict']['sku_penalty_v1']", self.handle_sku_penalty_v1)
        self.register_handler("['latest']['sensi_dict']['store_stock_v2']", self.handle_store_stock_v2)
        self.register_handler("['latest']['sensi_index']['wjex']", self.handle_wjex)
        self.register_handler("['latest']['sensi_index']['wjtag']", self.handle_wjtag)

        self.register_handler("['db']['global_dicts']['cover_area']['inc_latest']", self.handle_cover_area)
        self.register_handler("['db']['global_dicts']['store_time_status_v2']['inc_latest']", self.handle_store_time_status_v2)
        self.register_handler("['db']['global_dicts']['store_route']['inc_latest']", self.handle_store_route)
        self.register_handler("['db']['global_dicts']['query_promotion']['inc_latest']", self.handle_query_promotion)
        self.register_handler("['db']['global_dicts']['shop_penalty']['inc_latest']", self.handle_shop_penalty)
        self.register_handler("['db']['global_dicts']['jp_warehouse_county']['inc_latest']", self.handle_jp_warehouse_county)

        self.register_handler("['coupon']['activity']['update_inc']", self.handle_activity)
        self.register_handler("['coupon']['coupon']['update_inc']", self.handle_coupon)
        self.register_handler("['coupon']['ecard']['update_inc']", self.handle_ecard)
        self.register_handler("['coupon']['rebate']['update_inc']", self.handle_rebate)

        self.register_handler("['coupon']['financial']['update_inc']", self.handle_financial)
        self.register_handler("['coupon']['scene']['update_inc']", self.handle_scene)

    def handle_inc(self, value, partition):
        tms = int(time.mktime(time.strptime(value, self.DATA_TIME_FORMAT)))
        with open(json_dir, "r") as f:
            time_dic = json.load(f)
        current = int(time.mktime(time.strptime(time_dic["inc"][Module]["partition_" + str(partition)]["inc"], "%Y%m%d%H%M%S")))
        try:
            total_results[self.tags + "_" + partition] += 1
        except Exception:
            total_results[self.tags + "_" + partition] = 1
        inc_rule = time_rule['inc_rule']
        if current - tms >= inc_rule:
            try:
                inc[self.tags + "_" + partition] += 1
            except Exception:
                inc[self.tags + "_" + partition] = 1

    def handle_promotion_icon(self, value, partition):
        tms = int(time.mktime(time.strptime(value, self.DATA_TIME_FORMAT)))
        with open(json_dir, "r") as f:
            time_dic = json.load(f)
        current = int(time.mktime(time.strptime(time_dic["sensi"][Module]["partition_" + str(partition)]["promotion_icon"], "%Y%m%d%H%M%S")))
        promotion_icon_rule = time_rule['promotion_icon_rule']
        if current - tms >= promotion_icon_rule:
            try:
                promotion_icon[self.tags + "_" + partition] += 1
            except Exception:
                promotion_icon[self.tags + "_" + partition] = 1


    def handle_promotion_address(self, value, partition):
        tms = int(time.mktime(time.strptime(value, self.DATA_TIME_FORMAT)))
        with open(json_dir, "r") as f:
            time_dic = json.load(f)
        current = int(time.mktime(time.strptime(time_dic["sensi"][Module]["partition_" + str(partition)]["promotion_address"], "%Y%m%d%H%M%S")))
        promotion_address_rule = time_rule['promotion_address_rule']
        if current - tms >= promotion_address_rule:
            try:
                promotion_address[self.tags + "_" + partition] += 1
            except Exception:
                promotion_address[self.tags + "_" + partition] = 1

    def handle_wjex(self, value, partition):
        tms = int(time.mktime(time.strptime(value, self.DATA_TIME_FORMAT)))
        with open(json_dir, "r") as f:
            time_dic = json.load(f)
        current = int(time.mktime(time.strptime(time_dic["sensi"][Module]["partition_" + str(partition)]["wjex"],"%Y%m%d%H%M%S")))
        wjex_rule = time_rule['wjex_rule']
        if current - tms >= wjex_rule:
            try:
                wjex[self.tags + "_" + partition] += 1
            except Exception:
                wjex[self.tags + "_" + partition] = 1

    def handle_wjtag(self, value, partition):
        tms = int(time.mktime(time.strptime(value, self.DATA_TIME_FORMAT)))
        with open(json_dir, "r") as f:
            time_dic = json.load(f)
        current = int(time.mktime(time.strptime(time_dic["sensi"][Module]["partition_" + str(partition)]["wjtag"], "%Y%m%d%H%M%S")))
        wjtag_rule = time_rule['wjtag_rule']
        if current - tms >= wjtag_rule:
            try:
                wjtag[self.tags + "_" + partition] += 1
            except Exception:
                wjtag[self.tags + "_" + partition] = 1

    def handle_delivery_dict(self, value, partition):
        tms = int(time.mktime(time.strptime(value, self.DATA_TIME_FORMAT)))
        with open(json_dir, "r") as f:
            time_dic = json.load(f)
        current = int(time.mktime(time.strptime(time_dic["sensi"][Module]["partition_" + str(partition)]["delivery_dict"], "%Y%m%d%H%M%S")))
        delivery_dict_rule = time_rule['delivery_dict_rule']
        if current - tms >= delivery_dict_rule:
            try:
                delivery_dict[self.tags + "_" + partition] += 1
            except Exception:
                delivery_dict[self.tags + "_" + partition] = 1

    def handle_pop_par_stock(self, value, partition):
        tms = int(time.mktime(time.strptime(value, self.DATA_TIME_FORMAT)))
        with open(json_dir, "r") as f:
            time_dic = json.load(f)
        current = int(time.mktime(time.strptime(time_dic["sensi"][Module]["partition_" + str(partition)]["pop_par_stock"], "%Y%m%d%H%M%S")))
        pop_par_stock_rule = time_rule['pop_par_stock_rule']
        if current - tms >= pop_par_stock_rule:
            try:
                pop_par_stock[self.tags + "_" + partition] += 1
            except Exception:
                pop_par_stock[self.tags + "_" + partition] = 1

    def handle_sku_penalty_v1(self, value, partition):
        tms = int(time.mktime(time.strptime(value, self.DATA_TIME_FORMAT)))
        with open(json_dir, "r") as f:
            time_dic = json.load(f)
        current = int(time.mktime(time.strptime(time_dic["sensi"][Module]["partition_" + str(partition)]["sku_penalty_v1"], "%Y%m%d%H%M%S")))
        sku_penalty_v1_rule = time_rule['sku_penalty_v1_rule']
        if current - tms >= sku_penalty_v1_rule:
            try:
                sku_penalty_v1[self.tags + "_" + partition] += 1
            except Exception:
                sku_penalty_v1[self.tags + "_" + partition] = 1

    def handle_store_stock_v2(self, value, partition):
        tms = int(time.mktime(time.strptime(value, self.DATA_TIME_FORMAT)))
        with open(json_dir, "r") as f:
            time_dic = json.load(f)
        current = int(time.mktime(
            time.strptime(time_dic["sensi"][Module]["partition_" + str(partition)]["store_stock_v2"], "%Y%m%d%H%M%S")))
        store_stock_v2_rule = time_rule['store_stock_v2_rule']
        if current - tms >= store_stock_v2_rule:
            try:
                store_stock_v2[self.tags + "_" + partition] += 1
            except Exception:
                store_stock_v2[self.tags + "_" + partition] = 1

    def handle_cover_area(self, value, partition):
        tms = int(time.mktime(time.strptime(value, self.DATA_TIME_FORMAT)))
        with open(json_dir, "r") as f:
            time_dic = json.load(f)
        current = int(time.mktime(time.strptime(time_dic["sensi"][Module]["partition_" + str(partition)]["cover_area"], "%Y%m%d%H%M%S")))
        cover_area_rule = time_rule['cover_area_rule']
        if current - tms >= cover_area_rule:
            try:
                cover_area[self.tags + "_" + partition] += 1
            except Exception:
                cover_area[self.tags + "_" + partition] = 1

    def handle_store_time_status_v2(self, value, partition):
        tms = int(time.mktime(time.strptime(value, self.DATA_TIME_FORMAT)))
        with open(json_dir, "r") as f:
            time_dic = json.load(f)
        current = int(time.mktime(time.strptime(time_dic["sensi"][Module]["partition_" + str(partition)]["store_time_status_v2"], "%Y%m%d%H%M%S")))
        store_time_status_v2_rule = time_rule['store_time_status_v2_rule']
        if current - tms >= store_time_status_v2_rule:
            try:
                store_time_status_v2[self.tags + "_" + partition] += 1
            except Exception:
                store_time_status_v2[self.tags + "_" + partition] = 1

    def handle_store_route(self, value, partition):
        tms = int(time.mktime(time.strptime(value, self.DATA_TIME_FORMAT)))
        with open(json_dir, "r") as f:
            time_dic = json.load(f)
        current = int(time.mktime(
            time.strptime(time_dic["sensi"][Module]["partition_" + str(partition)]["store_route"], "%Y%m%d%H%M%S")))
        store_route_rule = time_rule['store_route_rule']
        if current - tms >= store_route_rule:
            try:
                store_route[self.tags + "_" + partition] += 1
            except Exception:
                store_route[self.tags + "_" + partition] = 1

    def handle_query_promotion(self, value, partition):
        tms = int(time.mktime(time.strptime(value, self.DATA_TIME_FORMAT)))
        with open(json_dir, "r") as f:
            time_dic = json.load(f)
        current = int(time.mktime(
            time.strptime(time_dic["sensi"][Module]["partition_" + str(partition)]["query_promotion"], "%Y%m%d%H%M%S")))
        query_promotion_rule = time_rule['query_promotion_rule']
        if current - tms >= query_promotion_rule:
            try:
                query_promotion[self.tags + "_" + partition] += 1
            except Exception:
                query_promotion[self.tags + "_" + partition] = 1

    def handle_shop_penalty(self, value, partition):
        tms = int(time.mktime(time.strptime(value, self.DATA_TIME_FORMAT)))
        with open(json_dir, "r") as f:
            time_dic = json.load(f)
        current = int(time.mktime(time.strptime(time_dic["sensi"][Module]["partition_" + str(partition)]["shop_penalty"], "%Y%m%d%H%M%S")))
        shop_penalty_rule = time_rule['shop_penalty_rule']
        if current - tms >= shop_penalty_rule:
            try:
                shop_penalty[self.tags + "_" + partition] += 1
            except Exception:
                shop_penalty[self.tags + "_" + partition] = 1

    def handle_jp_warehouse_county(self, value, partition):
        tms = int(time.mktime(time.strptime(value, self.DATA_TIME_FORMAT)))
        with open(json_dir, "r") as f:
            time_dic = json.load(f)
        current = int(time.mktime(time.strptime(time_dic["sensi"][Module]["partition_" + str(partition)]["jp_warehouse_county"], "%Y%m%d%H%M%S")))
        jp_warehouse_county_rule = time_rule['jp_warehouse_county_rule']
        if current - tms >= jp_warehouse_county_rule:
            try:
                jp_warehouse_county[self.tags + "_" + partition] += 1
            except Exception:
                jp_warehouse_county[self.tags + "_" + partition] = 1

    def handle_activity(self, value, partition):
        tms = int(time.mktime(time.strptime(value, self.DATA_TIME_FORMAT)))
        with open(json_dir, "r") as f:
            time_dic = json.load(f)
        current = int(time.mktime(time.strptime(time_dic["coupon"]["activity"], "%Y%m%d%H%M%S")))
        activity_rule = time_rule['activity_rule']
        if current - tms >= activity_rule:
            try:
                activity[self.tags + "_" + partition] += 1
            except Exception:
                activity[self.tags + "_" + partition] = 1

    def handle_coupon(self, value, partition):
        tms = int(time.mktime(time.strptime(value, self.DATA_TIME_FORMAT)))
        with open(json_dir, "r") as f:
            time_dic = json.load(f)
        current = int(time.mktime(time.strptime(time_dic["coupon"]["coupon"], "%Y%m%d%H%M%S")))
        coupon_rule = time_rule['coupon_rule']
        if current - tms >= coupon_rule:
            try:
                coupon[self.tags + "_" + partition] += 1
            except Exception:
                coupon[self.tags + "_" + partition] = 1

    def handle_ecard(self, value, partition):
        tms = int(time.mktime(time.strptime(value, self.DATA_TIME_FORMAT)))
        with open(json_dir, "r") as f:
            time_dic = json.load(f)
        current = int(time.mktime(time.strptime(time_dic["coupon"]["ecard"], "%Y%m%d%H%M%S")))
        ecard_rule = time_rule['ecard_rule']
        if current - tms >= ecard_rule:
            try:
                ecard[self.tags + "_" + partition] += 1
            except Exception:
                ecard[self.tags + "_" + partition] = 1

    def handle_financial(self, value, partition):
        tms = int(time.mktime(time.strptime(value, self.DATA_TIME_FORMAT)))
        with open(json_dir, "r") as f:
            time_dic = json.load(f)
        current = int(time.mktime(time.strptime(time_dic["coupon"]["financial"], "%Y%m%d%H%M%S")))
        financial_rule = time_rule['financial_rule']
        if current - tms >= financial_rule:
            try:
                financial[self.tags + "_" + partition] += 1
            except Exception:
                financial[self.tags + "_" + partition] = 1

    def handle_rebate(self, value, partition):
        tms = int(time.mktime(time.strptime(value, self.DATA_TIME_FORMAT)))
        with open(json_dir, "r") as f:
            time_dic = json.load(f)
        current = int(time.mktime(time.strptime(time_dic["coupon"]["rebate"], "%Y%m%d%H%M%S")))
        rebate_rule = time_rule['rebate_rule']
        if current - tms >= rebate_rule:
            try:
                rebate[self.tags + "_" + partition] += 1
            except Exception:
                rebate[self.tags + "_" + partition] = 1

    def handle_scene(self, value, partition):
        tms = int(time.mktime(time.strptime(value, self.DATA_TIME_FORMAT)))
        with open(json_dir, "r") as f:
            time_dic = json.load(f)
        current = int(time.mktime(time.strptime(time_dic["coupon"]["scene"], "%Y%m%d%H%M%S")))
        scene_rule = time_rule['scene_rule']
        if current - tms >= scene_rule:
            try:
                scene[self.tags + "_" + partition] += 1
            except Exception:
                scene[self.tags + "_" + partition] = 1


class Env(object):
    def __init__(self, agent_host, agent_port, debug):
        self.agent_host = agent_host
        self.agent_port = agent_port
        self.debug = debug


def get_info(value):
    global total_results
    global inc
    global promotion_address
    global delivery_dict
    global activity
    global coupon
    global ecard
    global financial
    global rebate
    global scene
    global promotion_icon
    global pop_par_stock
    global sku_penalty_v1
    global store_stock_v2
    global store_route
    global wjex
    global wjtag
    global cover_area
    global store_time_status_v2
    global store_route
    global query_promotion
    global shop_penalty
    global jp_warehouse_county
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
        print(p)

        # p = partition_28
        # p.lstrip("partition_") 28
        partition = p.lstrip("partition_")
        info.parse_json(ip, data, info.PART_MATCH_TB, partition, json_data)


def got_cmd(inner_module):
    cmd = ""
    global saigono_result
    global daijyoubu
    current_time = str(int(time.time()))
    for p, v in total_results.items():
        tmp = p.split("_")
        try:
            value = round((inc[p] / v), 2)
        except Exception:
            value = 0.0
        cmd += f'searchV7.{inner_module}.inc_p.avg,{int(current_time)},{value},hostname=partition_{tmp[1]},{tmp[0]},partition={tmp[1]}\n'
        if value >= threshold_rule:
            inc_key = f'searchV7.{inner_module}.inc_a.avg,{current_time},$,{tmp[0]}'
            try:
                saigono_result[inc_key] += 1
            except Exception:
                saigono_result[inc_key] = 1
        else:
            suki = f'searchV7.{inner_module}.inc_a.avg,{current_time},0,{tmp[0]}'
            daijyoubu.add(suki)

        try:
            value = round((promotion_address[p] / v), 2)
        except Exception:
            value = 0.0
        cmd += f'searchV7.{inner_module}.sensi_promotion_address_p.avg,{int(current_time)},{value},hostname=partition_{tmp[1]},{tmp[0]},partition={tmp[1]}\n'
        if value >= threshold_rule:
            address_key = f'searchV7.{inner_module}.sensi_promotion_address_a.avg,{current_time},$,{tmp[0]}'
            try:
                saigono_result[address_key] += 1
            except Exception:
                saigono_result[address_key] = 1
        else:
            suki = f'searchV7.{inner_module}.sensi_promotion_address_a.avg,{current_time},0,{tmp[0]}'
            daijyoubu.add(suki)

        try:
            value = round((delivery_dict[p] / v), 2)
        except Exception:
            value = 0.0
        cmd += f'searchV7.{inner_module}.sensi_delivery_dict_p.avg,{int(current_time)},{value},hostname=partition_{tmp[1]},{tmp[0]},partition={tmp[1]}\n'
        if value >= threshold_rule:
            dict_key = f'searchV7.{inner_module}.sensi_delivery_dict_a.avg,{current_time},$,{tmp[0]}'
            try:
                saigono_result[dict_key] += 1
            except Exception:
                saigono_result[dict_key] = 1
        else:
            suki = f'searchV7.{inner_module}.sensi_delivery_dict_a.avg,{current_time},0,{tmp[0]}'
            daijyoubu.add(suki)

        try:
            value = round((promotion_icon[p] / v), 2)
        except Exception:
            value = 0.0
        cmd += f'searchV7.{inner_module}.sensi_promotion_icon_p.avg,{int(current_time)},{value},hostname=partition_{tmp[1]},{tmp[0]},partition={tmp[1]}\n'
        if value >= threshold_rule:
            icon_key = f'searchV7.{inner_module}.sensi_promotion_icon_a.avg,{current_time},$,{tmp[0]}'
            try:
                saigono_result[icon_key] += 1
            except Exception:
                saigono_result[icon_key] = 1
        else:
            suki = f'searchV7.{inner_module}.sensi_promotion_icon_a.avg,{current_time},0,{tmp[0]}'
            daijyoubu.add(suki)

        try:
            value = round((pop_par_stock[p] / v), 2)
        except Exception:
            value = 0.0
        cmd += f'searchV7.{inner_module}.sensi_pop_par_stock_p.avg,{int(current_time)},{value},hostname=partition_{tmp[1]},{tmp[0]},partition={tmp[1]}\n'
        if value >= threshold_rule:
            pop_key = f'searchV7.{inner_module}.sensi_pop_par_stock_a.avg,{current_time},$,{tmp[0]}'
            try:
                saigono_result[pop_key] += 1
            except Exception:
                saigono_result[pop_key] = 1
        else:
            suki = f'searchV7.{inner_module}.sensi_pop_par_stock_a.avg,{current_time},0,{tmp[0]}'
            daijyoubu.add(suki)

        try:
            value = round((sku_penalty_v1[p] / v), 2)
        except Exception:
            value = 0.0
        cmd += f'searchV7.{inner_module}.sensi_sku_penalty_v1_p.avg,{int(current_time)},{value},hostname=partition_{tmp[1]},{tmp[0]},partition={tmp[1]}\n'
        if value >= threshold_rule:
            sku_key = f'searchV7.{inner_module}.sensi_sku_penalty_v1_a.avg,{current_time},$,{tmp[0]}'
            try:
                saigono_result[sku_key] += 1
            except Exception:
                saigono_result[sku_key] = 1
        else:
            suki = f'searchV7.{inner_module}.sensi_sku_penalty_v1_a.avg,{current_time},0,{tmp[0]}'
            daijyoubu.add(suki)

        try:
            value = round((store_stock_v2[p] / v), 2)
        except Exception:
            value = 0.0
        cmd += f'searchV7.{inner_module}.sensi_store_stock_v2_p.avg,{int(current_time)},{value},hostname=partition_{tmp[1]},{tmp[0]},partition={tmp[1]}\n'
        if value >= threshold_rule:
            store_key = f'searchV7.{inner_module}.sensi_store_stock_v2_a.avg,{current_time},$,{tmp[0]}'
            try:
                saigono_result[store_key] += 1
            except Exception:
                saigono_result[store_key] = 1
        else:
            suki = f'searchV7.{inner_module}.sensi_store_stock_v2_a.avg,{current_time},0,{tmp[0]}'
            daijyoubu.add(suki)

        try:
            value = round((wjex[p] / v), 2)
        except Exception:
            value = 0.0
        cmd += f'searchV7.{inner_module}.sensi_wjex_p.avg,{int(current_time)},{value},hostname=partition_{tmp[1]},{tmp[0]},partition={tmp[1]}\n'
        if value >= threshold_rule:
            wjex_key = f'searchV7.{inner_module}.sensi_wjex_a.avg,{current_time},$,{tmp[0]}'
            try:
                saigono_result[wjex_key] += 1
            except Exception:
                saigono_result[wjex_key] = 1
        else:
            suki = f'searchV7.{inner_module}.sensi_wjex_a.avg,{current_time},0,{tmp[0]}'
            daijyoubu.add(suki)

        try:
            value = round((wjtag[p] / v), 2)
        except Exception:
            value = 0.0
        cmd += f'searchV7.{inner_module}.sensi_wjtag_p.avg,{int(current_time)},{value},hostname=partition_{tmp[1]},{tmp[0]},partition={tmp[1]}\n'
        if value >= threshold_rule:
            wjtag_key = f'searchV7.{inner_module}.sensi_wjtag_a.avg,{current_time},$,{tmp[0]}'
            try:
                saigono_result[wjtag_key] += 1
            except Exception:
                saigono_result[wjtag_key] = 1
        else:
            suki = f'searchV7.{inner_module}.sensi_wjtag_a.avg,{current_time},0,{tmp[0]}'
            daijyoubu.add(suki)

        try:
            value = round((cover_area[p] / v), 2)
        except Exception:
            value = 0.0
        cmd += f'searchV7.{inner_module}.sensi_cover_area_p.avg,{int(current_time)},{value},hostname=partition_{tmp[1]},{tmp[0]},partition={tmp[1]}\n'
        if value >= threshold_rule:
            cover_key = f'searchV7.{inner_module}.sensi_cover_area_a.avg,{current_time},$,{tmp[0]}'
            try:
                saigono_result[cover_key] += 1
            except Exception:
                saigono_result[cover_key] = 1
        else:
            suki = f'searchV7.{inner_module}.sensi_cover_area_a.avg,{current_time},0,{tmp[0]}'
            daijyoubu.add(suki)

        try:
            value = round((store_time_status_v2[p] / v), 2)
        except Exception:
            value = 0.0
        cmd += f'searchV7.{inner_module}.sensi_store_time_status_v2_p.avg,{int(current_time)},{value},hostname=partition_{tmp[1]},{tmp[0]},partition={tmp[1]}\n'
        if value >= threshold_rule:
            status_key = f'searchV7.{inner_module}.sensi_store_time_status_v2_a.avg,{current_time},$,{tmp[0]}'
            try:
                saigono_result[status_key] += 1
            except Exception:
                saigono_result[status_key] = 1
        else:
            suki = f'searchV7.{inner_module}.sensi_store_time_status_v2_a.avg,{current_time},0,{tmp[0]}'
            daijyoubu.add(suki)

        try:
            value = round((store_route[p] / v), 2)
        except Exception:
            value = 0.0
        cmd += f'searchV7.{inner_module}.sensi_store_route_p.avg,{int(current_time)},{value},hostname=partition_{tmp[1]},{tmp[0]},partition={tmp[1]}\n'
        if value >= threshold_rule:
            route_key = f'searchV7.{inner_module}.sensi_store_route_a.avg,{current_time},$,{tmp[0]}'
            try:
                saigono_result[route_key] += 1
            except Exception:
                saigono_result[route_key] = 1
        else:
            suki = f'searchV7.{inner_module}.sensi_store_route_a.avg,{current_time},0,{tmp[0]}'
            daijyoubu.add(suki)

        try:
            value = round((query_promotion[p] / v), 2)
        except Exception:
            value = 0.0
        cmd += f'searchV7.{inner_module}.sensi_query_promotion_p.avg,{int(current_time)},{value},hostname=partition_{tmp[1]},{tmp[0]},partition={tmp[1]}\n'
        if value >= threshold_rule:
            query_key = f'searchV7.{inner_module}.sensi_query_promotion_a.avg,{current_time},$,{tmp[0]}'
            try:
                saigono_result[query_key] += 1
            except Exception:
                saigono_result[query_key] = 1
        else:
            suki = f'searchV7.{inner_module}.sensi_query_promotion_a.avg,{current_time},0,{tmp[0]}'
            daijyoubu.add(suki)

        try:
            value = round((shop_penalty[p] / v), 2)
        except Exception:
            value = 0.0
        cmd += f'searchV7.{inner_module}.sensi_shop_penalty_p.avg,{int(current_time)},{value},hostname=partition_{tmp[1]},{tmp[0]},partition={tmp[1]}\n'
        if value >= threshold_rule:
            shop_key = f'searchV7.{inner_module}.sensi_shop_penalty_a.avg,{current_time},$,{tmp[0]}'
            try:
                saigono_result[shop_key] += 1
            except Exception:
                saigono_result[shop_key] = 1
        else:
            suki = f'searchV7.{inner_module}.sensi_shop_penalty_a.avg,{current_time},0,{tmp[0]}'
            daijyoubu.add(suki)

        try:
            value = round((jp_warehouse_county[p] / v), 2)
        except Exception:
            value = 0.0
        cmd += f'searchV7.{inner_module}.sensi_jp_warehouse_county_p.avg,{int(current_time)},{value},hostname=partition_{tmp[1]},{tmp[0]},partition={tmp[1]}\n'
        if value >= threshold_rule:
            jp_key = f'searchV7.{inner_module}.sensi_jp_warehouse_county_a.avg,{current_time},$,{tmp[0]}'
            try:
                saigono_result[jp_key] += 1
            except Exception:
                saigono_result[jp_key] = 1
        else:
            suki = f'searchV7.{inner_module}.sensi_jp_warehouse_county_a.avg,{current_time},0,{tmp[0]}'
            daijyoubu.add(suki)

        try:
            value = round((activity[p] / v), 2)
        except Exception:
            value = 0.0
        cmd += f'searchV7.{inner_module}.coupon_activity_p.avg,{int(current_time)},{value},hostname=partition_{tmp[1]},{tmp[0]},partition={tmp[1]}\n'
        if value >= threshold_rule:
            activity_key = f'searchV7.{inner_module}.coupon_activity_a.avg,{current_time},$,{tmp[0]}'
            try:
                saigono_result[activity_key] += 1
            except Exception:
                saigono_result[activity_key] = 1
        else:
            suki = f'searchV7.{inner_module}.coupon_activity_a.avg,{current_time},0,{tmp[0]}'
            daijyoubu.add(suki)

        try:
            value = round((coupon[p] / v), 2)
        except Exception:
            value = 0.0
        cmd += f'searchV7.{inner_module}.coupon_coupon_p.avg,{int(current_time)},{value},hostname=partition_{tmp[1]},{tmp[0]},partition={tmp[1]}\n'
        if value >= threshold_rule:
            coupon_key = f'searchV7.{inner_module}.coupon_coupon_a.avg,{current_time},$,{tmp[0]}'
            try:
                saigono_result[coupon_key] += 1
            except Exception:
                saigono_result[coupon_key] = 1
        else:
            suki = f'searchV7.{inner_module}.coupon_coupon_a.avg,{current_time},0,{tmp[0]}'
            daijyoubu.add(suki)

        try:
            value = round((ecard[p] / v), 2)
        except Exception:
            value = 0.0
        cmd += f'searchV7.{inner_module}.coupon_ecard_p.avg,{int(current_time)},{value},hostname=partition_{tmp[1]},{tmp[0]},partition={tmp[1]}\n'
        if value >= threshold_rule:
            ecard_key = f'searchV7.{inner_module}.coupon_ecard_a.avg,{current_time},$,{tmp[0]}'
            try:
                saigono_result[ecard_key] += 1
            except Exception:
                saigono_result[ecard_key] = 1
        else:
            suki = f'searchV7.{inner_module}.coupon_ecard_a.avg,{current_time},0,{tmp[0]}'
            daijyoubu.add(suki)

        try:
            value = round((financial[p] / v), 2)
        except Exception:
            value = 0.0
        cmd += f'searchV7.{inner_module}.coupon_financial_p.avg,{int(current_time)},{value},hostname=partition_{tmp[1]},{tmp[0]},partition={tmp[1]}\n'
        if value >= threshold_rule:
            financial_key = f'searchV7.{inner_module}.coupon_financial_a.avg,{current_time},$,{tmp[0]}'
            try:
                saigono_result[financial_key] += 1
            except Exception:
                saigono_result[financial_key] = 1
        else:
            suki = f'searchV7.{inner_module}.coupon_financial_a.avg,{current_time},0,{tmp[0]}'
            daijyoubu.add(suki)

        try:
            value = round((rebate[p] / v), 2)
        except Exception:
            value = 0.0
        cmd += f'searchV7.{inner_module}.coupon_rebate_p.avg,{int(current_time)},{value},hostname=partition_{tmp[1]},{tmp[0]},partition={tmp[1]}\n'
        if value >= threshold_rule:
            rebate_key = f'searchV7.{inner_module}.coupon_rebate_a.avg,{current_time},$,{tmp[0]}'
            try:
                saigono_result[rebate_key] += 1
            except Exception:
                saigono_result[rebate_key] = 1
        else:
            suki = f'searchV7.{inner_module}.coupon_rebate_a.avg,{current_time},0,{tmp[0]}'
            daijyoubu.add(suki)

        try:
            value = round((scene[p] / v), 2)
        except Exception:
            value = 0.0
        cmd += f'searchV7.{inner_module}.coupon_scene_p.avg,{int(current_time)},{value},hostname=partition_{tmp[1]},{tmp[0]},partition={tmp[1]}\n'
        if value >= threshold_rule:
            sence_key = f'searchV7.{inner_module}.coupon_scene_a.avg,{current_time},$,{tmp[0]}'
            try:
                saigono_result[sence_key] += 1
            except Exception:
                saigono_result[sence_key] = 1
        else:
            suki = f'searchV7.{inner_module}.coupon_scene_a.avg,{current_time},0,{tmp[0]}'
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
    #print(args_lst)
    for i in args_lst:
        #print(i)
        th = threading.Thread(target=get_info, args=(i, ))
        th.start()
        th.join()


    print(total_results)

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
        #print(i)
        send_cmd(i)


if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option('-c', '--config', action='store', dest='config',
                      help='config_file', metavar='CONF_FILE', default='./original.json',)
    (options, args) = parser.parse_args()
    if not options.config:
        parser.error("缺少配置文件，如 -c xx.config")
    config = json.load(open(options.config))
    host = config['agent_host']
    port = config['agent_port']
    platform_list = config['platform']
    module_lst = config['module']
    debug = config['debug']
    time_rule = config['time_rule']
    json_dir = config['json_dir']
    threshold_rule = config['threshold_rule']
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
        daijyoubu = set()
        ip_partition = {}
        inc = {}
        promotion_address = {}
        delivery_dict = {}
        activity = {}
        coupon = {}
        ecard = {}
        financial = {}
        rebate = {}
        scene = {}
        promotion_icon = {}
        pop_par_stock = {}
        sku_penalty_v1 = {}
        store_stock_v2 = {}
        wjex = {}
        wjtag = {}
        cover_area = {}
        store_time_status_v2 = {}
        store_route = {}
        query_promotion = {}
        shop_penalty = {}
        jp_warehouse_county = {}
