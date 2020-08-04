# a = {}
# a["b"] = {"c": 123}
# print(a)
import json
with open("v7config.json", "r") as f:
    time_dic = json.load(f)
dd = time_dic["data_type"]
print(dd)
dd = dict.fromkeys(dd, dict())
print(dd)

# a = "ds_inc"
# print(a.split("_", maxsplit=1)[1])
# c = "coupon_query_promotion"
# print(c[7:])
# data_type = {"inc": {}}
# data_type["inc"]['dd'] = "dsaf"
# print(data_type)